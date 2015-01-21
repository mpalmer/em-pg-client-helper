class PG::EM::Client::Helper::Transaction
	include ::PG::EM::Client::Helper
	include ::EventMachine::Deferrable

	# Create a new transaction.  You shouldn't have to call this yourself;
	# `db_transaction` should create one and pass it to your block.
	def initialize(conn, opts, &blk)
		@conn       = conn
		@opts       = opts
		# This can be `nil` if the txn is in progress, or it will be
		# true or false to indicate success/failure of the txn
		@committed  = nil
		@retryable  = false

		DeferrableGroup.new do |dg|
			@dg = dg

			exec("BEGIN") do
				begin
					blk.call(self)
				rescue StandardError => ex
					rollback(ex)
				end
			end
		end.callback do
			rollback(RuntimeError.new("txn.commit was not called")) unless @committed
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

	# Mark the transaction as requiring the serializable isolation level.
	#
	# @param retryable [TrueClass, FalseClass] Whether or not the transaction
	#   should be retried if some sort of transaction-level failure occurs.
	#   Be careful enabling this, as the entire block will be re-run, including
	#   any code that creates side-effects elsewhere.
	#
	def serializable(retryable = false, &blk)
		@retryable = retryable
		exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &blk)
	end

	# Signal the database to commit this transaction.  You must do this
	# once you've completed your queries, it won't be called automatically
	# for you.  Once you've committed the transaction, you cannot use it
	# again; if you execute a query against a committed transaction, an
	# exception will be raised.
	#
	def commit
		if @committed.nil?
			trace_query("COMMIT")
			@conn.exec_defer("COMMIT", []).tap do |df|
				@dg.add(df)
			end.callback do
				@committed = true
				@dg.close
			end.errback do |ex|
				@committed = false
				@dg.fail(ex)
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
		if @committed.nil?
			exec("ROLLBACK") do
				@committed = false
				@dg.fail(ex)
				@dg.close
			end
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
	# @returns [EM::Deferrable] A deferrable that will be completed when this
	#   specific query finishes.
	#
	def exec(sql, values=[], &blk)
		unless @committed.nil?
			raise RuntimeError,
			      "Cannot execute a query in a transaction that has been closed"
		end

		trace_query(sql, values)
		@conn.exec_defer(sql, values).tap do |df|
			@dg.add(df)
			df.callback(&blk) if blk
		end.errback do |ex|
			rollback(ex)
		end
	end
	alias_method :exec_defer, :exec

	def trace_query(q, v=nil)
		$stderr.puts "#{@conn.inspect}: #{q} #{v.inspect}" if ENV['EM_PG_TXN_TRACE']
	end
end
