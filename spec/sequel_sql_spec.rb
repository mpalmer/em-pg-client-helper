require_relative './spec_helper'

describe "PG::EM::Client::Helper#sequel_sql" do
	it "assembles a simple post-return query correctly" do
		expect(sequel_sql { |db| db[:foo] }).
		  to eq("SELECT * FROM \"foo\"")
	end

	it "assembles a pre-complete query correctly" do
		expect(
		  sequel_sql do |db|
		    db[:foo].where { id > 20 }.delete
		  end
		).to eq("DELETE FROM \"foo\" WHERE (\"id\" > 20)")
	end

	it "bombs if we don't do anything" do
		expect { sequel_sql }.
		  to raise_error(PG::EM::Client::Helper::BadSequelError)
	end
end
