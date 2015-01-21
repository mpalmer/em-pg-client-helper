require 'pg/em'
require 'pg/em/connection_pool'

# Some helper methods to make working with em-pg-client slightly less like
# trying to build a house with a packet of seeds and a particle
# accelerator...  backwards.
#
module PG::EM::Client::Helper
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
end

require 'em-pg-client-helper/transaction'
require 'em-pg-client-helper/deferrable_group'
