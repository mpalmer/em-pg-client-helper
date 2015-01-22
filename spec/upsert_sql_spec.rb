require_relative './spec_helper'

describe "PG::EM::Client::Helper#upsert_sql" do
	it "assembles a simple query correctly" do
		expect(upsert_sql("foo", :wombat, :bar => "baz", :wombat => 42)).
		  to eq([
		    'WITH upsert AS (UPDATE "foo" SET "bar"=$1 WHERE "wombat"=$2 RETURNING *) ' +
		    'INSERT INTO "foo" ("bar","wombat") SELECT $1,$2 ' +
		    'WHERE NOT EXISTS (SELECT * FROM upsert)',
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
		    'WITH upsert AS (UPDATE "user_store_data" SET "data"=$4 ' +
		    'WHERE "id"=$1 AND "platform"=$2 AND "profile_type"=$3 RETURNING *) ' +
		    'INSERT INTO "user_store_data" ("id","platform","profile_type","data") ' +
		    'SELECT $1,$2,$3,$4 WHERE NOT EXISTS (SELECT * FROM upsert)',
		    [42, "wooden", "xyzzy", "ohai!"]
		  ])
	end
end
