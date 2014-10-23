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
	def insert_sql(tbl, params)
		keys = params.keys.map { |k| quote_identifier(k.to_s) }.join(',')
		vals = params.values
		val_places = (1..vals.length).to_a.map { |i| "$#{i}" }.join(',')

		["INSERT INTO #{quote_identifier(tbl)} (#{keys}) VALUES (#{val_places})", vals]
	end

	# Run an insert query, without having to write a great pile of SQL all by
	# yourself.
	#
	# Arguments:
	#
	#  * `db` -- A PG::EM::Client or PG::EM::ConnectionPool instance, against
	#    which all database operations will be executed.
	#
	#  * `tbl` -- The name of the table into which you wish to insert your data.
	#    This parameter will be automatically quoted, if necessary.
	#
	#  * `params` -- A hash containing the fields you wish to insert into
	#    (the keys of the hash) and the values to insert into each field (the
	#    values of the hash).  All field names and data will be automatically
	#    quoted and made safe, so you're automatically SQL injection-proof!
	#
	# This method returns the deferrable in which the query is being called;
	# this means you should attach the code to run after the query completes
	# with `#callback`, and you can attach an error handler with `#errback`
	# if you like.
	#
	def db_insert(db, tbl, params)
		db.exec_defer(*insert_sql(tbl, params))
	end

	# Execute code in a transaction.
	#
	# Calling this method opens up a transaction (by executing `BEGIN`), and
	# then runs the supplied block, passing in a transaction object which you
	# can use to execute SQL commands.  When the block completes, a `COMMIT`
	# will automatically be executed, unless you have manually called
	# `txn.commit` or `txn.rollback`.  Since `db_transaction` returns a
	# deferrable, you should use `#callback` to specify what to run after the
	# transaction completes.
	#
	# If an SQL error occurs during the transaction, the block's execution
	# will be aborted, a `ROLLBACK` will be executed, and the `#errback`
	# block (if defined) on the deferrable will be executed (rather than the
	# `#callback` block).
	#
	# Arguments:
	#
	#  * `db` -- A PG::EM::Client or PG::EM::ConnectionPool instance, against
	#    which all database operations will be executed.  If you pass a
	#    ConnectionPool, we will automatically hold a single connection for
	#    the transaction to complete against, so you don't have to worry
	#    about that, either.
	#
	#  * `blk` -- A block of code which will be executed within the context
	#    of the transaction.  This block will be passed a
	#    `PG::EM::Client::Helper::Transaction` instance, which has methods to
	#    allow you to commit or rollback the transaction, and execute SQL
	#    statements within the context of the transaction.
	#
	# Returns a deferrable object, on which you can call `#callback` and
	# `#errback` to define what to do when the transaction succeeds or fails,
	# respectively.
	#
	def db_transaction(db, opts = {}, &blk)
		if db.is_a? PG::EM::ConnectionPool
			db.__send__(:hold_deferrable) do |conn|
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
	def quote_identifier(id)
		"\"#{id.gsub(/"/, '""')}\""
	end

	class Transaction
		include ::PG::EM::Client::Helper
		include ::EventMachine::Deferrable
		
		# Create a new transaction.  You shouldn't have to call this yourself;
		# `db_transaction` should create one and pass it to your block.
		def initialize(conn, opts, &blk)
			@conn       = conn
			@opts       = opts
			@active     = true
			
			conn.exec_defer("BEGIN").callback do
				blk.call(self)
			end.errback { |ex| rollback(ex) }
		end

		# Signal the database to commit this transaction.  You must do this
		# once you've completed your queries, it won't be called automatically
		# for you.  Once you've committed the transaction, you cannot use it
		# again; if you execute a query against a committed transaction, an
		# exception will be raised.
		#
		def commit
			@conn.exec_defer("COMMIT").callback do
				self.succeed
				@active = false
			end.errback { |ex| rollback(ex) }
		end

		# Signal the database to abort this transaction.  You only need to
		# call this method if you need to rollback for some business logic
		# reason -- a rollback will be automatically performed for you in the
		# event of a database error or other exception.
		#
		def rollback(ex)
			@conn.exec_defer("ROLLBACK") do
				@active = false
				self.fail(ex)
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

			df = @conn.exec_defer(sql, values).
			       errback { |ex| rollback(ex) }
			df.callback(&blk) if blk
                        df
		end
	end
end
