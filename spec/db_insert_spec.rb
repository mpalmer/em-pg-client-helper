require_relative './spec_helper'

describe "PG::EM::Client::Helper#db_insert" do
	it "Calls the right SQL" do
		expect(db = double).
		  to receive(:exec_defer).
		  with(
		    'INSERT INTO "foo" ("bar") VALUES ($1)',
		    ['baz']
		  )
		
		db_insert(db, "foo", :bar => "baz")
	end
end
