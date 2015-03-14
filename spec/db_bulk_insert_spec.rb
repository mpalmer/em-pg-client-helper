require_relative './spec_helper'

describe "PG::EM::Client::Helper#db_bulk_insert" do
	let(:mock_conn) { double(PG::EM::Client) }

	it "inserts multiple records" do
		expect(dbl = double).to receive(:results).with(2)

		in_em do
			expect_query("BEGIN")
		   expect_query('INSERT INTO "foo" ' +
		                '(SELECT * FROM (VALUES (1, \'x\'), (3, \'y\')) ' +
		                'AS src ("bar", "baz") ' +
		                'WHERE NOT EXISTS ' +
		                '(SELECT 1 FROM "foo" AS dst ' +
		                'WHERE src."bar"=dst."bar" AND src."baz"=dst."baz"))',
		                [], 0.001, :succeed, Struct.new(:cmd_tuples).new(2)
		               )
		   expect_query("COMMIT")

			db_bulk_insert(mock_conn, "foo", [:bar, :baz], [[1, "x"], [3, "y"]]) do |count|
				dbl.results(count)
				EM.stop
			end
		end
	end
end
