require_relative './spec_helper'

describe "PG::EM::Client::Helper#db_bulk_insert" do
	let(:mock_conn) { double(PG::EM::Client) }
	def expect_unique_indexes_query(indexes)
		indexes.map! do |i|
			idxid = rand(1000000).to_s
			i.map { |n| {"idxid" => idxid, "name" => n.to_s} }
		end
		indexes.flatten!

		expect_query(/^SELECT a\.attrelid/,
		             [], 0.001, :succeed, indexes
		            )
	end

	it "inserts multiple records" do
		expect(dbl = double).to receive(:results).with(2)
		expect(dbl).to_not receive(:errback)

		query = 'INSERT INTO "foo" ("bar", "baz") ' +
			'(SELECT * FROM (VALUES (1, \'x\'), (3, \'y\')) ' +
			'AS src ("bar", "baz") ' +
			'WHERE NOT EXISTS ' +
			'(SELECT 1 FROM "foo" AS dst ' +
			'WHERE (src."bar"=dst."bar" AND src."baz"=dst."baz")))'

		in_em do
			expect_query("BEGIN")
			expect_unique_indexes_query([[:bar, :baz]])
			expect_query(query, [], 0.001, :succeed, Struct.new(:cmd_tuples).new(2))
			expect_query("COMMIT")

			db_bulk_insert(mock_conn, "foo", [:bar, :baz], [[1, "x"], [3, "y"]]) do |count|
				dbl.results(count)
				EM.stop
			end.errback { dbl.errback; EM.stop }
		end
	end

	it "inserts multiple records without a UNIQUE index" do
		expect(dbl = double).to receive(:results).with(2)
		expect(dbl).to_not receive(:errback)

		in_em do
			expect_query("BEGIN")
			expect_unique_indexes_query([])
			expect_query('INSERT INTO "foo" ("bar", "baz") VALUES (1, \'x\'), (3, \'y\')',
			             [], 0.001, :succeed, Struct.new(:cmd_tuples).new(2)
			            )
			expect_query("COMMIT")

			db_bulk_insert(mock_conn, "foo", [:bar, :baz], [[1, "x"], [3, "y"]]) do |count|
				dbl.results(count)
				EM.stop
			end.errback { dbl.errback; EM.stop }
		end
	end

	it "does nothing with an empty set of data" do
		expect(dbl = double).to receive(:results).with(0)
		expect(dbl).to_not receive(:errback)

		in_em do
			expect_query("BEGIN")
			expect_query("COMMIT")
			db_bulk_insert(mock_conn, "foo", [:bar, :baz], []) do |count|
				dbl.results(count)
				EM.stop
			end.errback { dbl.errback; EM.stop }
		end
	end
end
