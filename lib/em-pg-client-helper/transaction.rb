class PG::EM::Client::Helper::Transaction
	include ::PG::EM::Client::Helper
	include ::EventMachine::Deferrable

	# Create a new transaction.  You shouldn't have to call this yourself;
	# `db_transaction` should create one and pass it to your block.
	def initialize(conn, opts, &blk)
		@conn       = conn
		@opts       = opts
		@active     = true

		DeferrableGroup.new do |dg|
			@dg = dg

			df = conn.exec_defer("BEGIN").callback do
				begin
					blk.call(self)
				rescue StandardError => ex
					rollback(ex)
				end
			end.errback { |ex| rollback(ex) }

			@dg.add(df)
		end.callback do
			rollback(RuntimeError.new("txn.commit was not called"))
		end.errback do |ex|
			rollback(ex)
		end
	end

	# Signal the database to commit this transaction.  You must do this
	# once you've completed your queries, it won't be called automatically
	# for you.  Once you've committed the transaction, you cannot use it
	# again; if you execute a query against a committed transaction, an
	# exception will be raised.
	#
	def commit
		if @active
			df = @conn.exec_defer("COMMIT").callback do
				@active = false
				self.succeed
			end.errback { |ex| rollback(ex) }

			@dg.add(df)
		end
	end

	# Signal the database to abort this transaction.  You only need to
	# call this method if you need to rollback for some business logic
	# reason -- a rollback will be automatically performed for you in the
	# event of a database error or other exception.
	#
	def rollback(ex)
		if @active
			df = @conn.exec_defer("ROLLBACK") do
				@active = false
				self.fail(ex)
			end

			@dg.add(df)
		end
	end

	# Insert a row of data into the database table `tbl`, using the keys
	# from the `params` hash as the field names, and the values from the
	# `params` hash as the field data.  Once the query has completed,
	# `blk` will be called to allow the transaction to continue.
	#
	def insert(tbl, params, &blk)
		exec(*insert_sql(tbl, params), &blk)
	end

	# Execute an arbitrary block of SQL in `sql` within the transaction.
	# If you need to pass dynamic values to the query, those should be
	# given in `values`, and referenced in `sql` as `$1`, `$2`, etc.  The
	# given block will be called if and when the query completes
	# successfully.
	#
	def exec(sql, values=[], &blk)
		unless @active
			raise RuntimeError,
			      "Cannot execute a query in a transaction that has been closed"
		end

		@dg.add(
			@conn.exec_defer(sql, values).
			        tap { |df| df.callback(&blk) if blk }
		)
	end
	alias_method :exec_defer, :exec
end
