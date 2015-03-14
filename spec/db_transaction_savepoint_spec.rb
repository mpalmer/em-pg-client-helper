require_relative './spec_helper'

describe "PG::EM::Client::Helper::Transaction#savepoint" do
	let(:mock_conn) { double(PG::EM::Client) }

	it "executes through the savepoint in normal operation" do
		in_em do
			expect(SecureRandom).to receive(:uuid).and_return("faff")
			expect_query("BEGIN")
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["wombat"])
			expect_query('SAVEPOINT "faff"')
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			expect_query("COMMIT")
			in_transaction do |txn|
				txn.insert("foo", :bar => 'wombat') do
					txn.savepoint do
						txn.insert("foo", :bar => 'baz') do
							txn.commit
						end
					end.errback do
						txn.rollback  # Just to show this *doesn't* happen
					end
				end
			end
		end
	end

	it "rolls back to the savepoint after a failed INSERT" do
		in_em do
			expect(SecureRandom).to receive(:uuid).and_return("faff")
			expect_query("BEGIN")
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["wombat"])
			expect_query('SAVEPOINT "faff"')
			expect_query_failure('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			expect_query('ROLLBACK TO "faff"')
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["wibble"])
			expect_query("COMMIT")
			in_transaction do |txn|
				txn.insert("foo", :bar => 'wombat') do
					txn.savepoint do
						txn.insert("foo", :bar => 'baz') do
							txn.rollback  # Just to show this *doesn't* happen
						end
					end.errback do
						txn.insert("foo", :bar => 'wibble') do
							txn.commit
						end
					end
				end
			end
		end
	end

	it "issues ROLLBACK if query fails outside of savepoint" do
		in_em do
			expect(SecureRandom).to receive(:uuid).and_return("faff")
			expect_query("BEGIN")
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["wombat"])
			expect_query('SAVEPOINT "faff"')
			expect_query_failure('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			# I'd like to be able to verify that the savepoint IDs are the same
			# in both queries, but I'm not sure how.  Cross fingers!
			expect_query('ROLLBACK TO "faff"')
			expect_query_failure('INSERT INTO "foo" ("bar") VALUES ($1)', ["wibble"])
			expect_query("ROLLBACK")
			in_transaction do |txn|
				txn.insert("foo", :bar => 'wombat') do
					txn.savepoint do
						txn.insert("foo", :bar => 'baz') do
							txn.rollback  # Just to show this *doesn't* happen
						end
					end.errback do
						txn.insert("foo", :bar => 'wibble') do
							txn.commit
						end
					end
				end
			end
		end
	end

	it "handles nested SAVEPOINTs correctly" do
		in_em do
			expect(SecureRandom).to receive(:uuid).and_return("faff1")
			expect(SecureRandom).to receive(:uuid).and_return("faff2")
			expect_query("BEGIN")
			expect_query('INSERT INTO "foo" ("bar") VALUES ($1)', ["wombat"])
			expect_query('SAVEPOINT "faff1"')
			expect_query_failure('INSERT INTO "foo" ("bar") VALUES ($1)', ["baz"])
			# I'd like to be able to verify that the savepoint IDs are the same
			# in both queries, but I'm not sure how.  Cross fingers!
			expect_query('ROLLBACK TO "faff1"')
			
			expect_query('SAVEPOINT "faff2"')
			expect_query_failure('INSERT INTO "foo" ("bar") VALUES ($1)', ["wibble"])
			expect_query('ROLLBACK TO "faff2"')
			expect_query("COMMIT")

			in_transaction do |txn|
				txn.insert("foo", :bar => 'wombat') do
					txn.savepoint do
						txn.insert("foo", :bar => 'baz') do
							txn.rollback  # Just to show this *doesn't* happen
						end
					end.errback do
						txn.savepoint do
							txn.insert("foo", :bar => 'wibble') do
								txn.rollback  # Another "shouldn't happen"
							end.errback do
								txn.commit
							end
						end
					end
				end
			end
		end
	end
end
