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
	def quote_identifier(id)
		"\"#{id.gsub(/"/, '""')}\""
	end
end

require_relative 'em-pg-client-helper/transaction'
require_relative 'em-pg-client-helper/deferrable_group'

