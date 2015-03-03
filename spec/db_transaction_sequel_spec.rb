require_relative './spec_helper'

describe "PG::EM::Client::Helper::Transaction#sequel" do
	let(:mock_conn) { double(PG::EM::Client) }

	it "runs a simple UPSERT correctly" do
		in_em do
			expect_query("BEGIN")
			expect_query("SELECT * FROM \"foo\"")
			expect_query("COMMIT")
			in_transaction do |txn|
				txn.sequel do |db|
					db[:foo]
				end.callback do
					txn.commit
				end
			end
		end
	end

	it "rolls back after a failed attempt" do
		in_em do
			expect_query("BEGIN")
			expect_query_failure("SELECT * FROM \"foo\"")
			expect_query("ROLLBACK")
			in_transaction do |txn|
				txn.sequel do |db|
					db[:foo]
				end.callback do
					txn.commit
				end
			end
		end
	end
end
