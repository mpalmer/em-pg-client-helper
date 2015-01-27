require_relative './spec_helper'

describe "PG::EM::Client::Helper#db_transaction#upsert" do
	let(:mock_conn) { double(PG::EM::Client) }
	let(:mock_query) do
		[
		 'WITH update_query AS (UPDATE "foo" SET "bar"=$1 WHERE "wombat"=$2 RETURNING *), ' +
		 'insert_query AS (INSERT INTO "foo" ("bar","wombat") SELECT $1,$2 WHERE NOT EXISTS (SELECT * FROM update_query) RETURNING *) ' +
		 'SELECT * FROM update_query UNION SELECT * FROM insert_query',
		 ["baz", 42]
		]
	end
	let(:mock_query_opts) do
		["foo", :wombat, { :bar => "baz", :wombat => 42 }]
	end

	it "runs a simple UPSERT correctly" do
		in_em do
			expect_query("BEGIN")
			expect_query(*mock_query)
			expect_query("COMMIT")
			in_transaction do |txn|
				txn.upsert(*mock_query_opts) do
					txn.commit
				end
			end
		end
	end

	it "rolls back after a failed attempt" do
		in_em do
			expect_query("BEGIN")
			expect_query_failure(*mock_query)
			expect_query("ROLLBACK")
			in_transaction do |txn|
				txn.upsert(*mock_query_opts) do
					txn.commit
				end
			end
		end
	end

	it "tries again if the first failure was a PG::UniqueViolation" do
		in_em do
			expect_query("BEGIN")
			expect_query_failure(*mock_query, PG::UniqueViolation.new("OMFG"))
			expect_query(*mock_query)
			expect_query("COMMIT")

			in_transaction do |txn|
				txn.upsert(*mock_query_opts) do
					txn.commit
				end
			end
		end
	end

	it "explodes if the second time fails" do
		in_em do
			expect_query("BEGIN")
			expect_query_failure(*mock_query, PG::UniqueViolation.new("OMFG"))
			expect_query_failure(*mock_query)
			expect_query("ROLLBACK")

			in_transaction do |txn|
				txn.upsert(*mock_query_opts) do
					txn.commit
				end
			end
		end
	end

	it "explodes if the second failure was a PG::UniqueViolation" do
		in_em do
			expect_query("BEGIN")
			expect_query_failure(*mock_query, PG::UniqueViolation.new("OMFG"))
			expect_query_failure(*mock_query, PG::UniqueViolation.new("OMFG"))
			expect_query("ROLLBACK")

			in_transaction do |txn|
				txn.upsert(*mock_query_opts) do
					txn.commit
				end
			end
		end
	end
end
