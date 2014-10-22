require_relative './spec_helper'

describe "PG::EM::Client::Helper#db_transaction" do
	def mock_query_chain(queries, fail_on = -1, &blk)
		mock_conn = double(PG::EM::Client)

		queries.each_with_index do |q, i|
			df = EM::DefaultDeferrable.new

			ex = expect(mock_conn).
			  to receive(:exec_defer).
			  with(*q).
			  and_return(df)

			# Rollback expects a yield
			if q == ["ROLLBACK"]
				ex.and_yield()
			end
			
			if i == fail_on
				df.fail
			else
				df.succeed
			end
		end

		EM.run_block do
			db_transaction(mock_conn, &blk)
		end
	end

	it "runs a BEGIN/COMMIT cycle by default" do
		mock_query_chain([["BEGIN"], ["COMMIT"]]) do
			EM::DefaultDeferrable.new.tap { |df| df.succeed }
		end
	end
	
	it "rolls back if BEGIN fails" do
		mock_query_chain([["BEGIN"], ["ROLLBACK"]], 0) do
			EM::DefaultDeferrable.new.tap { |df| df.succeed }
		end
	end
	
	it "rolls back if COMMIT fails" do
		mock_query_chain([["BEGIN"], ["COMMIT"], ["ROLLBACK"]], 1) do
			EM::DefaultDeferrable.new.tap { |df| df.succeed }
		end
	end

	it "runs a simple INSERT" do
		mock_query_chain([
		  ["BEGIN"],
		  ['INSERT INTO "foo" ("bar") VALUES ($1)',
		   ["baz"]
		  ],
		  ["COMMIT"]
		]) do |txn|
			txn.insert("foo", :bar => 'baz')
		end
	end

	it "rolls back after a failed INSERT" do
		mock_query_chain(
		  [
		    ["BEGIN"],
		    ['INSERT INTO "foo" ("bar") VALUES ($1)',
		     ["baz"]
		    ],
		    ["ROLLBACK"]
		  ],
		  1
		) do |txn|
			txn.insert("foo", :bar => 'baz')
		end
	end
end
