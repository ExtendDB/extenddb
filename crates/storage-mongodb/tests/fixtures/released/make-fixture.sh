#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Make the fixture of one released MongoDB catalog:
#
#   make-fixture.sh <version> <container> <mongo port> <api port> [dotted-ttl]
#
# Starts mongo:7 as a one-node replica set in a container named <container>,
# with its port and the API port published on 127.0.0.1 only, runs `init`
# with the image extenddb/extenddb-mongodb:<version>, starts its server,
# provisions account 123456789012 with the IAM user cu-tester, two inline
# policies (one with a dynamodb:ResourceTag condition) and an access key
# through `extenddb manage`, creates the table seed.py describes, stops the
# server and writes both databases to <version>.json beside this script
# with dump.js. The containers are removed at the end.
#
# With `dotted-ttl` it also creates the table cu_dotted with TTL on the
# attribute `exp.at` and then adds the index releases before v0.1.12 created
# for it, `idx_ttl_exp.at` on `item_data.exp.at.N` (sparse;
# `v0.1.11:crates/storage-mongodb/src/metadata_engine.rs:293-301`), which
# v0.1.12 and later do not create; the file is then dotted-ttl.json.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
version=$1; m=$2; mport=$3; aport=$4; variant=${5:-}
server="$m-server"
work="$(mktemp -d)"
cleanup() {
    docker rm -f "$server" "$m" >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT

docker run -d --name "$m" -p "127.0.0.1:$mport:$mport" -p "127.0.0.1:$aport:18443" \
    -v "$here:/fixtures:ro" mongo:7 mongod --replSet rs0 --bind_ip_all --port "$mport" >/dev/null
for _ in $(seq 1 60); do
    docker exec "$m" mongosh --quiet --port "$mport" --eval 'db.runCommand({ping: 1}).ok' 2>/dev/null \
        | grep -qx 1 && break
    sleep 1
done
docker exec "$m" mongosh --quiet --port "$mport" \
    --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:$mport'}]})" >/dev/null
for _ in $(seq 1 60); do
    docker exec "$m" mongosh --quiet --port "$mport" --eval 'rs.status().myState' 2>/dev/null \
        | grep -qx 1 && break
    sleep 1
done

printf '[storage]\nbackend = "mongodb"\n\n[storage.mongodb]\nconnection_string = "mongodb://localhost:%s/?replicaSet=rs0"\n' \
    "$mport" > "$work/extenddb.toml"
password=FixtureAdminPassw0rd1
docker run --rm --network "container:$m" --user "$(id -u):$(id -g)" -v "$work:/var/lib/extenddb" \
    -e EXTENDDB_ADMIN_USER=admin -e EXTENDDB_ADMIN_PASSWORD=$password \
    "extenddb/extenddb-mongodb:$version" init --backend mongodb \
    --config /var/lib/extenddb/extenddb.toml --overwrite --bind-addr 0.0.0.0 \
    --tls-san localhost --tls-san 127.0.0.1 > "$work/init.log" 2>&1
docker run -d --name "$server" --network "container:$m" --user "$(id -u):$(id -g)" \
    -v "$work:/var/lib/extenddb" "extenddb/extenddb-mongodb:$version" >/dev/null
for _ in $(seq 1 120); do
    curl -sk "https://127.0.0.1:$aport/health" | grep -q healthy && break
    sleep 0.5
done
mg() {
    docker exec -e EXTENDDB_PASSWORD=$password "$server" extenddb manage --user admin \
        --config /var/lib/extenddb/extenddb.toml "$@"
}
mg create-account --account-id 123456789012 --account-name cu-seed >/dev/null
mg create-user --account-id 123456789012 --user-name cu-tester >/dev/null
mg put-user-policy --account-id 123456789012 --user-name cu-tester --policy-name dynamodb-full \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}' \
    >/dev/null
mg put-user-policy --account-id 123456789012 --user-name cu-tester --policy-name deny-prod \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:DeleteTable","Resource":"*","Condition":{"StringEquals":{"dynamodb:ResourceTag/env":"prod"}}}]}' \
    >/dev/null
mg create-access-key --account-id 123456789012 --user-name cu-tester > "$work/access-key.json"
key=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['access_key_id'])" "$work/access-key.json")
secret=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['secret_access_key'])" "$work/access-key.json")
python3 "$here/seed.py" "https://127.0.0.1:$aport" "$key" "$secret"
out="$here/$version.json"
if [ "$variant" = dotted-ttl ]; then
    python3 "$here/seed.py" "https://127.0.0.1:$aport" "$key" "$secret" cu_dotted exp.at
    docker exec "$m" mongosh --quiet --port "$mport" --eval '
        const d = db.getSiblingDB("extenddb_catalog");
        const t = d.tables.findOne({ "_id.table_name": "cu_dotted" });
        db.getSiblingDB("extenddb_data").getCollection("_ddb_" + t.table_id)
            .createIndex({ "item_data.exp.at.N": 1 }, { name: "idx_ttl_exp.at", sparse: true });' >/dev/null
    out="$here/dotted-ttl.json"
fi
docker stop "$server" >/dev/null
docker exec "$m" mongosh --quiet --port "$mport" --file /fixtures/dump.js > "$out"
echo "wrote $out"
