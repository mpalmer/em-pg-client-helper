require_relative './spec_helper'

describe "PG::EM::Client::Helper#upsert_sql" do
	it "assembles a simple query correctly" do
		expect(upsert_sql("foo", :wombat, :bar => "baz", :wombat => 42)).
		  to eq([
		    'WITH update_query AS (UPDATE "foo" SET "bar"=$1 WHERE "wombat"=$2 RETURNING *), ' +
		    'insert_query AS (INSERT INTO "foo" ("bar","wombat") SELECT $1,$2 WHERE NOT EXISTS (SELECT * FROM update_query) RETURNING *) ' +
		    'SELECT * FROM update_query UNION SELECT * FROM insert_query',
		    ["baz", 42]
		  ])
	end

	it "assembles a more complex query correctly" do
		expect(
		  upsert_sql(
		    "user_store_data",
		    [:id, :platform, :profile_type],
		    :id           => 42,
		    :platform     => 'wooden',
		    :profile_type => 'xyzzy',
		    :data         => "ohai!"
		  )
		).to eq([
		    'WITH update_query AS (UPDATE "user_store_data" SET "data"=$4 WHERE "id"=$1 AND "platform"=$2 AND "profile_type"=$3 RETURNING *), ' +
		    'insert_query AS (INSERT INTO "user_store_data" ("id","platform","profile_type","data") ' +
		    'SELECT $1,$2,$3,$4 WHERE NOT EXISTS (SELECT * FROM update_query) RETURNING *) ' +
		    'SELECT * FROM update_query UNION SELECT * FROM insert_query',
		    [42, "wooden", "xyzzy", "ohai!"]
		  ])
	end
end
