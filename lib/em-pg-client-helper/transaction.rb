require 'securerandom'

# Represents a database transaction, and contains all of the methods which
# can be used to execute queries within the transaction connection.
#
class PG::EM::Client::Helper::Transaction
	include ::PG::EM::Client::Helper
	include ::EventMachine::Deferrable

	# Raised when you attempt to execute a query in a transaction which has
	# been finished (either by COMMIT or ROLLBACK).
	#
	# @since 2.0.0
	#
	class ClosedError < StandardError; end

	# Create a new transaction.  You shouldn't have to call this yourself;
	# `db_transaction` should create one and pass it to your block.
	#
	# @param conn [PG::EM::Connection] The connection to execute all commands
	#   against.  If using a connection pool, this connection needs to have
	#   been taken out of the pool (using something like `#hold_deferred`) so
	#   that no other concurrently-operating code can accidentally send
	#   queries down the connection (that would be, in a word, *bad*).
	#
	# @param opts [Hash] Zero or more optional parameters that adjust the
	#   initial state of the transaction.  For full details of the available
	#   options, see {PG::EM::Client::Helper#db_transaction}.
	#
	# @raise [ArgumentError] If an unknown isolation level is specified.
	#
	def initialize(conn, opts = {}, &blk)
		@conn                  = conn
		@opts                  = opts
		@finished              = nil
		@retryable             = opts[:retry]
		@autorollback_on_error = true
		@savepoint_stack       = []

		DeferrableGroup.new do |dg|
			@dg = dg

			begin_query = case opts[:isolation]
				when :serializable
					"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"
				when :repeatable_read
					"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"
				when :read_committed
					"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"
				when :read_uncommitted
					"BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
				when nil
					"BEGIN"
				else
					raise ArgumentError,
					      "Unknown value for :isolation option: #{opts[:isolation].inspect}"
			end

			if opts[:deferrable]
				begin_query += " DEFERRABLE"
			end

			exec(begin_query) do
				begin
					blk.call(self)
				rescue StandardError => ex
					rollback(ex)
				end
			end
		end.callback do
			rollback(RuntimeError.new("txn.commit was not called")) unless @finished
			self.succeed
		end.errback do |ex|
			if @retryable and [PG::TRSerializationFailure].include?(ex.class)
				self.class.new(conn, opts, &blk).callback do
					self.succeed
				end.errback do |ex|
					self.fail(ex)
				end
			else
				self.fail(ex)
			end
		end
	end

	# Signal the database to commit this transaction.  You must do this
	# once you've completed your queries, it won't be called automatically
	# for you.  Once you've committed the transaction, you cannot use it
	# again; if you execute a query against a committed transaction, an
	# exception will be raised.
	#
	def commit
		unless @finished
			trace_query("COMMIT")
			@conn.exec_defer("COMMIT", []).tap do |df|
				@dg.add(df)
			end.callback do
				@finished = true
				@dg.close
				yield if block_given?
			end.errback do
				@finished = true
				@dg.close
			end
		end
	end

	# Signal the database to abort this transaction.  You only need to
	# call this method if you need to rollback for some business logic
	# reason -- a rollback will be automatically performed for you in the
	# event of a database error or other exception.
	#
	def rollback(ex)
		unless @finished
			if @savepoint_stack.empty?
				exec("ROLLBACK") do
					@finished = true
					@dg.fail(ex)
					@dg.close
				end
			else
				sp = @savepoint_stack.pop
				exec("ROLLBACK TO \"#{sp[:savepoint]}\"")
				sp[:deferrable].fail(ex)
				@dg.close
				@dg = sp[:parent_deferrable_group]
			end
		end
	end

	# Manage the "rollback on the failure of a single query" behaviour.
	#
	# The default behaviour of a transaction, when a query fails, is for
	# the transaction to automatically be rolled back and the rest of the
	# statements to not be executed.  In **ALMOST** every case, this is the
	# correct behaviour.  However, there are some corner cases in which you
	# want to be able to avoid this behaviour, and will manually react to
	# the transaction failure in some way.  In that case, you can set this
	# to `false` and the transaction will not automatically fail.
	#
	# Given that pretty much the only thing you can do when a query fails,
	# other than abort the transaction, is to rollback to a savepoint, you
	# might want to look at {#savepoint} before you try using this.
	#
	# @since 2.0.0
	#
	attr_accessor :autorollback_on_error

	# Setup a "savepoint" within the transaction.
	#
	# A savepoint is, as the name suggests, kinda like a "saved game", in an
	# SQL transaction.  If a query fails within a transaction, normally all
	# you can do is rollback and abort the entire transaction.  Savepoints
	# give you another option: roll back to the savepoint, and try again.
	#
	# So, that's what this method does.  Inside of the block passed to
	# `#savepoint`, if any query fails, instead of rolling back the entire
	# transaction, we instead only rollback to the savepoint, and execution
	# continues by executing the `errback` callbacks defined on the savepoint
	# deferrable.
	#
	# @return [EM::Deferrable]
	#
	# @since 2.0.0
	#
	def savepoint(&blk)
		savepoint = SecureRandom.uuid
		parent_dg = @dg
		DeferrableGroup.new do |dg|
			@dg = dg

			dg.callback do
				@dg = parent_dg
				@dg.close
			end

			exec("SAVEPOINT \"#{savepoint}\"").tap do |df|
				@savepoint_stack << { :savepoint => savepoint,
				                      :deferrable => df,
				                      :parent_deferrable_group => parent_dg
				                    }

				df.callback(&blk) if blk
			end
		end
	end


	# Generate SQL statements via Sequel, and run the result against the
	# database.  Very chic.
	#
	# @see {PG::EM::Client::Helper#sequel_sql}
	#
	# @return [EM::Deferrable]
	#
	def sequel(&blk)
		exec(sequel_sql(&blk))
	end

	# Insert a row of data into the database table `tbl`, using the keys
	# from the `params` hash as the field names, and the values from the
	# `params` hash as the field data.  Once the query has completed,
	# `blk` will be called to allow the transaction to continue.
	#
	def insert(tbl, params, &blk)
		exec(*insert_sql(tbl, params), &blk)
	end

	# Efficiently perform a "bulk" insert of multiple rows.
	#
	# When you have a large quantity of data to insert into a table, you don't
	# want to do it one row at a time -- that's *really* inefficient.  On the
	# other hand, if you do one giant multi-row insert statement, the insert
	# will fail if *any* of the rows causes a constraint failure.  What to do?
	#
	# Well, here's our answer: try to insert all the records at once.  If that
	# fails with a constraint violation, then split the set of records in half
	# and try to bulk insert each of those halves.  Recurse in this fashion until
	# you only have one record to insert.
	#
	# @param tbl [#to_sym] the name of the table into which you wish to insert
	#   your data.
	#
	# @param columns [Array<#to_sym>] the columns into which each record of data
	#   will be inserted.
	#
	# @param rows [Array<Array<Object>>] the values to insert.  Each entry in
	#   the outermost array is a row of data; the elements of each of these inner
	#   arrays corresponds to the column in the same position in the `columns`
	#   array.  **NOTE**: we don't do any checking to make sure you're giving
	#   us the correct list of values for each row.  Thus, if you give us a
	#   row array that has too few, or too many, entries, the database will puke.
	#
	# @yield [Integer] Once the insert has completed, the number of rows that
	#   were successfully inserted (that may be less than `rows.length` if
	#   there were any constraint failures) will be yielded to the block.
	#
	# @since 2.0.0
	#
	def bulk_insert(tbl, columns, rows, &blk)
		columns.map!(&:to_sym)

		EM::Completion.new.tap do |df|
			@dg.add(df)
			df.callback(&blk) if blk

			unique_index_columns_for_table(tbl) do |indexes|
				# Guh hand-hacked SQL is fugly... but what I'm doing is so utterly
				# niche that Sequel doesn't support it.
				q_tbl = usdb.literal(tbl.to_sym)
				q_cols = columns.map { |c| usdb.literal(c) }

				# If there are any unique indexes which the set of columns to
				# be inserted into *doesn't* completely cover, we need to error
				# out, because that will cause no rows (or, at most, one row) to
				# be successfully inserted.  In *theory*, a unique index with all-but-one
				# row inserted *could* work, but that would only work if every value
				# inserted was different, but quite frankly, I think that's a wicked
				# corner case I'm not going to go *anywhere* near.
				#
				unless indexes.all? { |i| (i - columns).empty? }
					ex = ArgumentError.new("Unique index columns not covered by data columns")
					if @autorollback_on_error
						df.fail(ex)
						rollback(ex)
					else
						df.fail(ex)
					end
				else
					subselect_where = indexes.map do |idx|
						"(" + idx.map do |c|
							"src.#{usdb.literal(c)}=dst.#{usdb.literal(c)}"
						end.join(" AND ") + ")"
					end.join(" OR ")

					subselect = "SELECT 1 FROM #{q_tbl} AS dst WHERE #{subselect_where}"

					vals = rows.map do |row|
						"(" + row.map { |v| usdb.literal(v) }.join(", ") + ")"
					end.join(", ")
					q = "INSERT INTO #{q_tbl} (SELECT * FROM (VALUES #{vals}) " +
						 "AS src (#{q_cols.join(", ")}) WHERE NOT EXISTS (#{subselect}))"
					exec(q) do |res|
						df.succeed(res.cmd_tuples)
					end
				end
			end.errback do |ex|
				df.fail(ex)
			end
		end
	end

	# Run an upsert inside a transaction.
	#
	# @see {PG::EM::Client::Helper#upsert_sql} for all the parameters.
	#
	# @return [EM::Deferrable]
	#
	# @yield [PG::Result] the row of data that has been inserted/updated.
	#
	def upsert(*args, &blk)
		db_upsert(@conn, *args).tap do |df|
			df.callback(&blk) if block_given?
		end.errback do |ex|
			rollback(ex)
		end
	end

	# Execute an arbitrary block of SQL in `sql` within the transaction.
	# If you need to pass dynamic values to the query, those should be
	# given in `values`, and referenced in `sql` as `$1`, `$2`, etc.  The
	# given block will be called if and when the query completes
	# successfully.
	#
	# @return [EM::Deferrable] A deferrable that will be completed when this
	#   specific query finishes.
	#
	def exec(sql, values=[], &blk)
		trace_query(sql, values)

		if @finished
			raise ClosedError,
			      "Cannot execute a query in a transaction that has been closed"
		end

		@conn.exec_defer(sql, values).tap do |df|
			@dg.add(df)
			df.callback(&blk) if blk
		end.errback do |ex|
			rollback(ex) if @autorollback_on_error
		end
	end
	alias_method :exec_defer, :exec

	private

	# Trace queries as they happen, if `ENV['EM_PG_TXN_TRACE']` is set.
	#
	def trace_query(q, v=nil)
		$stderr.puts "#{@conn.inspect}: #{q} #{v.inspect}" if ENV['EM_PG_TXN_TRACE']
	end

	# The Utility Sequel Data Base.  A mock PgSQL Sequel database that we can
	# call methods like `#literal` on, for easy quoting.
	#
	def usdb
		@usdb ||= Sequel.connect("mock://postgres")
	end

	# Find the unique indexes for a table, and yield the columns in each.
	#
	# This is used to work out what sets of fields to match on when doing
	# bulk inserts.
	#
	# @yield [Array<Array<Symbol>>] Each element in the yielded array is a
	#   list of the fields which make up a single unique index.
	#
	def unique_index_columns_for_table(t)
		q = "SELECT a.attrelid AS idxid,
		            a.attname  AS name,
		            (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
		               FROM pg_catalog.pg_attrdef d
		              WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS default
		       FROM pg_catalog.pg_attribute AS a
		       JOIN pg_catalog.pg_index AS i ON a.attrelid=i.indexrelid
		       JOIN pg_catalog.pg_class AS c ON c.oid=i.indrelid
		      WHERE c.relname=#{usdb.literal(t.to_s)}
		        AND a.attnum > 0
		        AND NOT a.attisdropped
		        AND i.indisunique"

		exec(q) do |res|
			yield(res.to_a.each_with_object(Hash.new { |h,k| h[k] = [] }) do |r, h|
				# Skip auto-incrementing fields; they can take care of themselves
				next if r["default"] =~ /^nextval/
				h[r["idxid"]] << r["name"].to_sym
			end.values)
		end
	end
end
