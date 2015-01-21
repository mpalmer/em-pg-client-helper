require_relative './spec_helper'

describe "PG::EM::Client::Helper#db_transaction" do
	let(:mock_conn) { double(PG::EM::Client) }

	def expect_query_failure(q, args=nil, err=nil, exec_time = 0.001)
		err ||= RuntimeError.new("Dummy failure")
		expect_query(q, args, exec_time, :fail, err)
	end

	def expect_query(q, args=nil, exec_time = 0.001, disposition = :succeed, *disp_opts)
		df = EM::DefaultDeferrable.new

		expect(mock_conn)
		  .to receive(:exec_defer)
		  .with(*[q, args].compact)
		  .and_return(df)
		  .ordered

		EM.add_timer(exec_time) do
			df.__send__(disposition, *disp_opts)
		end
	end

	def in_transaction(&blk)
		db_transaction(mock_conn, &blk).callback { EM.stop }.errback { EM.stop }
	end

	def in_em
		EM.run do
			EM.add_timer(0.5) { EM.stop; raise "test timeout" }
			yield
		end
	end

	it "runs a BEGIN/COMMIT cycle by default" do
		in_em do
			expect_query("BEGIN")
			expect_query("COMMIT")
			in_transaction do |txn|
				txn.commit
			end
		end
	end

	it "rolls back if BEGIN fails" do
		in_em do
			expect_query_failure("BEGIN")
			expect_query("ROLLBACK")
			in_transaction do |txn|
				txn.commit
			end
		end
	end

	it "doesn't roll back if COMMIT fails" do
		in_em do
			expect_query("BEGIN")
			expect_query_failure("COMMIT")
			in_transaction do |txn|
				txn.commit
			end
		end
	end

	it "runs a simple INSERT correctly" do
		in_em do
			expect_query("BEGIN")
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			expect_query("COMMIT")
			in_transaction do |txn|
				txn.insert("foo", :bar => 'baz') do
					txn.commit
				end
			end
		end
	end

	it "rolls back after a failed INSERT" do
		in_em do
			expect_query("BEGIN")
			expect_query_failure('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			expect_query("ROLLBACK")
			in_transaction do |txn|
				txn.insert("foo", :bar => 'baz') do
					txn.commit
				end
			end
		end
	end

	it "runs nested inserts in the right order" do
		in_em do
			expect_query("BEGIN")
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ['baz'])
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ['wombat'])
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ['quux'])
			expect_query("COMMIT")

			in_transaction do |txn|
				txn.insert("foo", :bar => 'baz') do
					txn.insert("foo", :bar => 'wombat') do
						txn.insert("foo", :bar => 'quux') do
							txn.commit
						end
					end
				end
			end
		end
	end

	it "is robust against slow queries" do
		# All tests up to now *could* have just passed "by accident", because
		# the queries were running fast enough to come out in order, even if
		# we weren't properly synchronising.  However, by making the second
		# insert run slowly, we should be able to be confident that we're
		# properly running the queries in order.
		in_em do
			expect_query("BEGIN")
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ['baz'])
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ['wombat'], 0.1)
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ['quux'])
			expect_query("COMMIT")

			in_transaction do |txn|
				txn.insert("foo", :bar => 'baz') do
					txn.insert("foo", :bar => 'wombat') do
						txn.insert("foo", :bar => 'quux') do
							txn.commit
						end
					end
				end
			end
		end
	end

	it "doesn't COMMIT if we rolled back" do
		in_em do
			expect_query("BEGIN")
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			expect_query("ROLLBACK")
			in_transaction do |txn|
				txn.insert("foo", :bar => 'baz') do
					txn.rollback("Because I can")
				end
			end
		end
	end

	it "catches exceptions" do
		in_em do
			expect_query("BEGIN")
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			expect_query("ROLLBACK")
			in_transaction do |txn|
				txn.insert("foo", :bar => 'baz')
				raise "OMFG"
			end
		end
	end

	it "retries if it gets an error during the transaction" do
		in_em do
			expect_query("BEGIN")
			expect_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", [])
			expect_query_failure('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"], PG::TRSerializationFailure.new("OMFG!"))
			expect_query("ROLLBACK")
			expect_query("BEGIN")
			expect_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", [])
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			expect_query("COMMIT")

			in_transaction do |txn|
				txn.serializable(true) do
					txn.insert("foo", :bar => 'baz') do
						txn.commit
					end
				end
			end
		end
	end

	it "retries if it gets an error on commit" do
		in_em do
			expect_query("BEGIN")
			expect_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", [])
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			expect_query_failure("COMMIT", nil, PG::TRSerializationFailure.new("OMFG!"))
			expect_query("BEGIN")
			expect_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", [])
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			expect_query("COMMIT")

			in_transaction do |txn|
				txn.serializable(true) do
					txn.insert("foo", :bar => 'baz') do
						txn.commit
					end
				end
			end
		end
	end
end
