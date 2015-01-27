require_relative './spec_helper'

describe "PG::EM::Client::Helper#db_upsert" do
	let(:mock_query) do
		[
		 'WITH update_query AS (UPDATE "foo" SET "bar"=$1 WHERE "wombat"=$2 RETURNING *), ' +
		 'insert_query AS (INSERT INTO "foo" ("bar","wombat") SELECT $1,$2 WHERE NOT EXISTS (SELECT * FROM update_query) RETURNING *) ' +
		 'SELECT * FROM update_query UNION SELECT * FROM insert_query',
		 ["baz", 42]
		]
	end

	it "works first time" do
		expect(db = double).
		  to receive(:exec_defer).
		  with(*mock_query).
		  and_return(::EM::DefaultDeferrable.new.tap { |df| df.succeed })

		df = db_upsert(db, "foo", :wombat, :bar => "baz", :wombat => 42)
		expect(df.instance_variable_get(:@deferred_status)).to eq(:succeeded)
	end

	it "works on retry" do
		expect(db = double).
		  to receive(:exec_defer).
		  with(*mock_query).
		  ordered.
		  and_return(
		    ::EM::DefaultDeferrable.new.tap do |df|
		      df.fail(PG::UniqueViolation.new("OMFG"))
		    end
		  )

		expect(db).
		  to receive(:exec_defer).
		  with(*mock_query).
		  ordered.
		  and_return(::EM::DefaultDeferrable.new.tap { |df| df.succeed })

		df = db_upsert(db, "foo", :wombat, :bar => "baz", :wombat => 42)
		expect(df.instance_variable_get(:@deferred_status)).to eq(:succeeded)
	end

	it "fails on a different error" do
		expect(db = double).
		  to receive(:exec_defer).
		  with(*mock_query).
		  ordered.
		  and_return(
		    ::EM::DefaultDeferrable.new.tap do |df|
		      df.fail(PG::UndefinedTable.new("OMFG"))
		    end
		  )

		df = db_upsert(db, "foo", :wombat, :bar => "baz", :wombat => 42)
		expect(df.instance_variable_get(:@deferred_status)).to eq(:failed)
	end
end
