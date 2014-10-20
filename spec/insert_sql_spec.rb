require_relative './spec_helper'

describe "PG::EM::Client::Helper#insert_sql" do
	it "assembles a simple query correctly" do
		expect(insert_sql("foo", :bar => "baz", :wombat => 42)).
		  to eq([
		    'INSERT INTO "foo" ("bar","wombat") VALUES ($1,$2)',
		    ["baz", 42]
		  ])
	end
end
