require 'pg/em'
require 'pg/em/connection_pool'
require 'sequel'

# Some helper methods to make working with em-pg-client slightly less like
# trying to build a house with a packet of seeds and a particle
# accelerator...  backwards.
#
module PG::EM::Client::Helper
	# Sequel-based SQL generation.
	#
	# While we could spend a lot of time writing code to generate various
	# kinds of SQL "by hand", it would be wasted effort, since the Sequel
	# database toolkit gives us a complete, and *extremely* powerful, SQL
	# generation system, which is already familiar to a great many
	# programmers (or, at least, many great programmers).
	#
	# Hence, rather than reinvent the wheel, we simply drop Sequel in.
	#
	# Anything you can do with an instance of `Sequel::Database` that
	# produces a single SQL query, you can almost certainly do with this
	# method.
	#
	# Usage is quite simple: calling this method will yield a pseudo-database
	# object to the block you pass.  You can then call whatever methods you
	# like against the database object, and when you're done and the block
	# you passed completes, we'll return the SQL that Sequel generated.
	#
	# @yield [sqldb] to allow you to call whatever Sequel methods you like to
	#   generate the desired SQL.
	#
	# @yieldparam [Sequel::Database] call whatever you like against this to
	#   cause Sequel to generate the result you want.
	#
	# @return [String] the generated SQL.
	#
	# @raise [PG::EM::Client::Helper::BadSequelError] if your calls against
	#   the yielded database object would result in no SQL being generated,
	#   or more than one SQL statement.
	#
	# @example A simple select
	#   sequel(db) do |sqldb|
	#     sqldb[:artists]
	#   end
	#   # => "SELECT * FROM artists"
	#
	# @example Delete some rows
	#   sequel(db) do |sqldb|
	#     sqldb[:foo].where { { col.sql_number % 3 => 0 } }.delete
	#   end.callback do |res|
	#     logger.info "Deleted #{res.cmd_tuples}"
	#   end
	#   # => "DELETE FROM foo WHERE col % 3 = 0"
	#
	# @example A very complicated select
	#   sequel do |sqldb|
	#     sqldb[:posts].select_all(:posts).
	#       select_append(
	#         Sequel.
	#           function(:array_agg, :campaigns__name).
	#           distinct.
	#           as(:campaigns_names)
	#         ).
	#       join(:posts_terms, :post_id => :posts__id).
	#       join(:terms, :id => :posts_terms__term_id).
	#       join(:categories_terms, :term_id => :terms__id).
	#       join(:campaigns_categories, :category_id => :categories_terms__category_id).
	#       join(:campaigns, :id => :campaigns_categories__campaign_id).
	#       group_by(:posts__id)
	#   end
	#   # ... you don't want to know.
	#
	def sequel_sql
		sqldb = Thread.current[:em_pg_client_sequel_db] ||= Sequel.connect("mock://postgres", :keep_reference => false)
		ret = yield sqldb if block_given?
		sqls = sqldb.sqls

		if sqls.empty?
			sqls = [ret.sql] rescue ret
		end

		if sqls.nil? or sqls.empty?
			raise PG::EM::Client::Helper::BadSequelError,
			      "Your block did not generate an SQL statement"
		end

              # remove transaction commands from our sqls list
              transaction_values = ['BEGIN', 'COMMIT']
              sqls -= transaction_values

		if sqls.length > 1
			raise PG::EM::Client::Helper::BadSequelError,
			      "Your block generated multiple SQL statements"
		end

		sqls.first
	end

	# Generate Sequel, and run it against the database connection provided.
	#
	# This is the all-singing variant of {#sequel_sql} -- in addition to
	# generating the SQL, we also run the result against the database
	# connection you pass.
	#
	# @see {#sequel_sql}
	#
	# @yield [sqldb] to allow you to call whatever sequel methods you like to
	#   generate the desired SQL.
	#
	# @yieldparam [Sequel::Database] call whatever you like against this to
	#   cause Sequel to generate the result you want.
	#
	# @return [EM::Deferrable] the callbacks attached to this deferrable will
	#   receive a `PG::Result` when the query completes successfully, or else
	#   the errbacks on this deferrable will be called in the event of an
	#   error.
	#
	# @raise [PG::EM::Client::Helper::BadSequelError] if your calls against
	#   the yielded database object would result in no SQL being generated,
	#   or more than one SQL statement.
	#
	# @example A simple select
	#   sequel(db) do |sqldb|
	#     sqldb[:artists]
	#   end.callback do |res|
	#     ...
	#   end.errback do |ex|
	#     logger.error "Query failed (#{ex.class}): #{ex.message}"
	#   end
	#
	# @example A very complicated select
	#   sequel do |sqldb|
	#     sqldb[:posts].select_all(:posts).
	#       select_append(
	#         Sequel.
	#           function(:array_agg, :campaigns__name).
	#           distinct.
	#           as(:campaigns_names)
	#         ).
	#       join(:posts_terms, :post_id => :posts__id).
	#       join(:terms, :id => :posts_terms__term_id).
	#       join(:categories_terms, :term_id => :terms__id).
	#       join(:campaigns_categories, :category_id => :categories_terms__category_id).
	#       join(:campaigns, :id => :campaigns_categories__campaign_id).
	#       group_by(:posts__id)
	#   end.callback do |res|
	#     ...
	#   end
	#
	# @example Delete some rows
	#   sequel(db) do |sqldb|
	#     sqldb[:foo].where { { col.sql_number % 3 => 0 } }.delete
	#   end.callback do |res|
	#     logger.info "Deleted #{res.cmd_tuples}"
	#   end
	#
	def db_sequel(db, &blk)
		db.exec_defer(sequel_sql(&blk))
	end

	# Generate SQL for an insert statement into `tbl`, with the fields and
	# data given by the keys and values, respectively, of `params`.  Returns
	# a two-element array consisting of the parameterised SQL as the first
	# element, and the array of parameters as the second element.
	#
	# @param tbl [#to_s]
	#
	# @param params [Hash<#to_s, Object>]
	#
	def insert_sql(tbl, params)
		keys = params.keys.map { |k| quote_identifier(k.to_s) }.join(',')
		vals = params.values
		val_places = (1..vals.length).to_a.map { |i| "$#{i}" }.join(',')

		["INSERT INTO #{quote_identifier(tbl.to_s)} (#{keys}) VALUES (#{val_places})", vals]
	end

	# Run an insert query, without having to write a great pile of SQL all by
	# yourself.
	#
	# @param db [PG::EM::Client, PG::EM::ConnectionPool] the connection
	#   against which all database operations will be run.
	#
	# @param tbl [#to_s] the name of the table into which you wish to insert
	#   your data.  This parameter will be automatically quoted, if
	#   necessary.
	#
	# @param params [Hash<#to_s, Object>] the fields you wish to insert into
	#   (the keys of the hash) and the values to insert into each field (the
	#   values of the hash).  All field names and data will be automatically
	#   quoted and made safe, so you're automatically SQL injection-proof!
	#
	# @return [EM::Deferrable] the deferrable in which the query is being
	#   called; this means you should attach the code to run after the query
	#   completes with `#callback`, and you can attach an error handler with
	#   `#errback` if you like.
	#
	def db_insert(db, tbl, params)
		db.exec_defer(*insert_sql(tbl, params))
	end

	# Efficiently perform a "bulk" insert of multiple rows.
	#
	# When you have a large quantity of data to insert into a table, you
	# don't want to do it one row at a time -- that's *really* inefficient.
	# On the other hand, if you do one giant multi-row insert statement, the
	# insert will fail if *any* of the rows causes a constraint failure.
	# What to do?
	#
	# Well, here's our answer: try to insert all the records at once.  If
	# that fails with a constraint violation, then split the set of records
	# in half and try to bulk insert each of those halves.  Recurse in this
	# fashion until you only have one record to insert.
	#
	# @param db [PG::EM::Client, PG::EM::ConnectionPool] the connection
	#   against which the insert wil be run.
	#
	# @param tbl [#to_sym] see
	#   {PG::EM::Client::Helper::Transaction#bulk_insert}.
	#
	# @param columns [Array<#to_sym>] see
	#   {PG::EM::Client::Helper::Transaction#bulk_insert}.
	#
	# @param rows [Array<Array<Object>>] see
	#   {PG::EM::Client::Helper::Transaction#bulk_insert}.
	#
	# @return [EM::Deferrable] the deferrable in which the query is being
	#   called; once the bulk insert completes successfully, the deferrable
	#   will succeed with the number of rows that were successfully inserted.
	#   If the insert could not be completed, the deferrable will fail
	#   (`#errback`) with the exception.
	#
	# @note for details on the `tbl`, `columns`, and `rows` parameters, see
	#   {PG::EM::Client::Helper::Transaction#bulk_insert}.
	#
	# @since 2.0.0
	#
	def db_bulk_insert(db, tbl, columns, rows, &blk)
		EM::Completion.new.tap do |df|
			df.callback(&blk) if blk

			db_transaction(db) do |txn|
				txn.bulk_insert(tbl, columns, rows) do |count|
					txn.commit do
						df.succeed(count)
					end
				end
			end.errback do |ex|
				df.fail(ex)
			end
		end
	end

	# @!macro upsert_params
	#
	#   @param tbl [#to_s] The name of the table on which to operate.
	#
	#   @param key [#to_s, Array<#to_s>] A field (or list of fields) which
	#     are the set of values that uniquely identify the record to be
	#     updated, if it exists.  You only need to specify the field names
	#     here, as the values which will be used in the query will be taken
	#     from the `data`.
	#
	#   @param data [Hash<#to_s, Object>] The fields and values to insert into
	#     the database, or to set in the existing record.
	#
	#   @raise [ArgumentError] if a field is specified in `key` but which
	#     does not exist in `data`.

	# An "upsert" is a kind of crazy hybrid "update if the record exists,
	# insert it if it doesn't" query.  It isn't part of the SQL standard,
	# but it is such a common idiom that we're keen to support it.
	#
	# The trick is that it's actually two queries in one.  We try to do an
	# `UPDATE` first, and if that doesn't actually update anything, then we
	# try an `INSERT`.  Since it is two separate queries, though, there is still
	# a small chance that the query will fail with a `PG::UniqueViolation`, so
	# your code must handle that.
	#
	# As an added bonus, the SQL that this method generates will, when executed,
	# return the complete row that has been inserted or updated.
	#
	# @macro upsert_params
	#
	# @return [Array<String, Array<Object>>] A two-element array, the first
	#   of which is a string containing the literal SQL to be executed, while
	#   the second element is an array containing the values, in order
	#   corresponding to the placeholders in the SQL.
	#
	def upsert_sql(tbl, key, data)
		tbl = quote_identifier(tbl)
		insert_keys = data.keys.map { |k| quote_identifier(k.to_s) }
		unique_keys = (key.is_a?(Array) ? key : [key])
		unique_keys.map! { |k| quote_identifier(k.to_s) }
		update_keys = insert_keys - unique_keys

		unless (bad_keys = unique_keys - insert_keys).empty?
			raise ArgumentError,
			      "These field(s) were mentioned in the key list, but were not in the data set: #{bad_keys.inspect}"
		end

		values = data.values
		# field-to-placeholder mapping
		i = 0
		fp_map = Hash[insert_keys.map { |k| i += 1; [k, "$#{i}"] }]

		update_values = update_keys.map { |k| "#{k}=#{fp_map[k]}" }.join(',')
		select_values = unique_keys.map { |k| "#{k}=#{fp_map[k]}" }.join(' AND ')
		update_query = "UPDATE #{tbl} SET #{update_values} WHERE #{select_values} RETURNING *"

		insert_query = "INSERT INTO #{tbl} (#{fp_map.keys.join(',')}) " +
		               "SELECT #{fp_map.values.join(',')}"

		["WITH update_query AS (#{update_query}), " +
		      "insert_query AS (#{insert_query} " +
		        "WHERE NOT EXISTS (SELECT * FROM update_query) " +
		        "RETURNING *) " +
		      "SELECT * FROM update_query UNION SELECT * FROM insert_query",
		 data.values
		]
	end

	# Run an upsert query.
	#
	# @see #upsert_sql
	#
	# Apply an upsert (update-or-insert) against a given database connection or
	# connection pool, handling the (rarely needed) unique violation that can
	# result.
	#
	# @param db [PG::EM::Client, PG::EM::ConnectionPool] the connection
	#   against which all database operations will be run.
	#
	# @macro upsert_params
	#
	# @return [EM::Deferrable] the deferrable in which the query is being
	#   called; this means you should attach the code to run after the query
	#   completes with `#callback`, and you can attach an error handler with
	#   `#errback` if you like.
	#
	# @yield [PG::Result] the row of data that has been inserted/updated.
	#
	def db_upsert(db, tbl, key, data)
		q = upsert_sql(tbl, key, data)

		::EM::DefaultDeferrable.new.tap do |df|
			db.exec_defer(*q).callback do |r|
				df.succeed(r)
			end.errback do |ex|
				if ex.is_a?(PG::UniqueViolation)
					db.exec_defer(*q).callback do |r|
						df.succeed(r)
					end.errback do |ex|
						df.fail(ex)
					end
				else
					df.fail(ex)
				end
			end
		end
	end

	# Execute code in a transaction.
	#
	# Calling this method opens up a transaction (by executing `BEGIN`), and
	# then runs the supplied block, passing in a transaction object which you
	# can use to execute SQL commands.  Once the transaction is finished,
	# `COMMIT` or `ROLLBACK` will be sent to the DB server to complete the
	# transaction, depending on whether or not any errors (query failures or
	# Ruby exceptions) appeared during the transaction.  You can also
	# manually call `txn.rollback(reason)` if you want to signal that the
	# transaction should be rolled back.
	#
	# You should use `#callback` and `#errback` against the deferrable that
	# `db_transaction` returns to specify what to run after the transaction
	# completes successfully or fails, respectively.
	#
	# @param db [PG::EM::Client, PG::EM::ConnectionPool] the connection
	#   against which the transaction will be executed.  If you pass a
	#   `ConnectionPool`, we will automatically hold a single connection for
	#   the transaction to complete against, so you don't have to worry about
	#   that, either.
	#
	# @param opts [Hash] Zero or more options which change the behaviour of the
	#   transaction.
	#
	# @option opts [Symbol] :isolation An isolation level for the transaction.
	#   Valid values are `:serializable`, `:repeatable_read`,
	#   `:read_committed`, and `:read_uncommitted`.  The last two of these
	#   are pointless to use and are included only for completeness, as
	#   PostgreSQL's default isolation level is `:read_committed`, and
	#   `:read_uncommitted` is equivalent to `:read_committed`.
	#
	# @option opts [TrueClass, FalseClass] :retry Whether or not to retry the
	#   transaction if it fails for one of a number of transaction-internal
	#   reasons.
	#
	# @option opts [TrueClass, FalseClass] :deferrable If set, enables the
	#   `DEFERRABLE` transaction mode.  For details of what this is, see the
	#   `SET TRANSACTION` command documentation in the PostgreSQL manual.
	#
	# @param blk [Proc] code which will be executed within the context of the
	#   transaction.  This block will be passed a
	#   {PG::EM::Client::Helper::Transaction} instance, which has methods to
	#   allow you to commit or rollback the transaction, and execute SQL
	#   statements within the context of the transaction.
	#
	# @return [EM::Deferrable] on which you can call `#callback` and
	#   `#errback` to define what to do when the transaction succeeds or
	#   fails, respectively.
	#
	# @raise [ArgumentError] If an unrecognised value for the `:isolation`
	#   option is given.
	#
	# @note Due to the way that transactions detect when they are completed,
	#   every deferrable in the scope of the transaction must be generated
	#   by the transaction.  That is, you cannot use objects other than the
	#   transaction asynchronously.  This is a known limitation, and will be
	#   addressed in a future version of this library.
	#
	def db_transaction(db, opts = {}, &blk)
		if db.is_a? PG::EM::ConnectionPool
			db.__send__(:hold_deferred) do |conn|
				::PG::EM::Client::Helper::Transaction.new(conn, opts, &blk)
			end
		else
			::PG::EM::Client::Helper::Transaction.new(db, opts, &blk)
		end
	end

	# Take a PgSQL identifier (anything that isn't data, basically) and quote
	# it so that it will always be valid, no matter what insanity someone's
	# decided to put in their names.
	#
	# @param id [String]
	#
	# @return [String] just like `id`, but with added quoting.
	#
	def quote_identifier(id)
		"\"#{id.gsub(/"/, '""')}\""
	end

	# Indicates that the sequel mock-database was not manipulated in such a way
	# as to produce exactly one SQL statement.
	#
	# @see {#sequel_sql}
	#
	class BadSequelError < StandardError; end
end

require 'em-pg-client-helper/transaction'
require 'em-pg-client-helper/deferrable_group'
