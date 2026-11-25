PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" (
    pk TEXT NOT NULL,
    sk_s TEXT,
    sk_n TEXT,
    sk_b BLOB,
    item_data TEXT NOT NULL,
    PRIMARY KEY (pk, sk_s)
);
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p0','s00',NULL,NULL,'{"expires":{"N":"4102444800"},"gpk":{"S":"g0"},"gsk":{"N":"0"},"lsk":{"S":"l09"},"payload":{"S":"item 0"},"pk":{"S":"p0"},"sk":{"S":"s00"}}');
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p1','s01',NULL,NULL,'{"expires":{"N":"4102444801"},"gpk":{"S":"g1"},"gsk":{"N":"1"},"lsk":{"S":"l08"},"payload":{"S":"item 1"},"pk":{"S":"p1"},"sk":{"S":"s01"}}');
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p2','s02',NULL,NULL,'{"expires":{"N":"4102444802"},"gpk":{"S":"g0"},"gsk":{"N":"2"},"lsk":{"S":"l07"},"payload":{"S":"item 2"},"pk":{"S":"p2"},"sk":{"S":"s02"}}');
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p0','s03',NULL,NULL,'{"expires":{"N":"4102444803"},"gpk":{"S":"g1"},"gsk":{"N":"3"},"lsk":{"S":"l06"},"payload":{"S":"item 3"},"pk":{"S":"p0"},"sk":{"S":"s03"}}');
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p1','s04',NULL,NULL,'{"expires":{"N":"4102444804"},"gpk":{"S":"g0"},"gsk":{"N":"4"},"lsk":{"S":"l05"},"payload":{"S":"item 4"},"pk":{"S":"p1"},"sk":{"S":"s04"}}');
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p2','s05',NULL,NULL,'{"expires":{"N":"4102444805"},"gpk":{"S":"g1"},"gsk":{"N":"5"},"lsk":{"S":"l04"},"payload":{"S":"item 5"},"pk":{"S":"p2"},"sk":{"S":"s05"}}');
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p0','s06',NULL,NULL,'{"expires":{"N":"4102444806"},"gpk":{"S":"g0"},"gsk":{"N":"6"},"lsk":{"S":"l03"},"payload":{"S":"item 6"},"pk":{"S":"p0"},"sk":{"S":"s06"}}');
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p1','s07',NULL,NULL,'{"expires":{"N":"4102444807"},"gpk":{"S":"g1"},"gsk":{"N":"7"},"lsk":{"S":"l02"},"payload":{"S":"item 7"},"pk":{"S":"p1"},"sk":{"S":"s07"}}');
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p2','s08',NULL,NULL,'{"expires":{"N":"4102444808"},"gpk":{"S":"g0"},"gsk":{"N":"8"},"lsk":{"S":"l01"},"payload":{"S":"item 8"},"pk":{"S":"p2"},"sk":{"S":"s08"}}');
INSERT INTO "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" VALUES('p0','s09',NULL,NULL,'{"expires":{"N":"4102444809"},"gpk":{"S":"g1"},"gsk":{"N":"9"},"lsk":{"S":"l00"},"payload":{"S":"item 9"},"pk":{"S":"p0"},"sk":{"S":"s09"}}');
CREATE TABLE "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" (
    pk TEXT NOT NULL,
    sk_s TEXT,
    sk_n TEXT,
    sk_b BLOB,
    base_pk TEXT NOT NULL,
    base_sk_s TEXT,
    base_sk_n TEXT,
    base_sk_b BLOB,
    item_data TEXT NOT NULL,
    PRIMARY KEY (pk, base_pk, base_sk_s)
);
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p0','l09',NULL,NULL,'p0','s00',NULL,NULL,'{"lsk":{"S":"l09"},"pk":{"S":"p0"},"sk":{"S":"s00"}}');
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p1','l08',NULL,NULL,'p1','s01',NULL,NULL,'{"lsk":{"S":"l08"},"pk":{"S":"p1"},"sk":{"S":"s01"}}');
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p2','l07',NULL,NULL,'p2','s02',NULL,NULL,'{"lsk":{"S":"l07"},"pk":{"S":"p2"},"sk":{"S":"s02"}}');
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p0','l06',NULL,NULL,'p0','s03',NULL,NULL,'{"lsk":{"S":"l06"},"pk":{"S":"p0"},"sk":{"S":"s03"}}');
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p1','l05',NULL,NULL,'p1','s04',NULL,NULL,'{"lsk":{"S":"l05"},"pk":{"S":"p1"},"sk":{"S":"s04"}}');
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p2','l04',NULL,NULL,'p2','s05',NULL,NULL,'{"lsk":{"S":"l04"},"pk":{"S":"p2"},"sk":{"S":"s05"}}');
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p0','l03',NULL,NULL,'p0','s06',NULL,NULL,'{"lsk":{"S":"l03"},"pk":{"S":"p0"},"sk":{"S":"s06"}}');
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p1','l02',NULL,NULL,'p1','s07',NULL,NULL,'{"lsk":{"S":"l02"},"pk":{"S":"p1"},"sk":{"S":"s07"}}');
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p2','l01',NULL,NULL,'p2','s08',NULL,NULL,'{"lsk":{"S":"l01"},"pk":{"S":"p2"},"sk":{"S":"s08"}}');
INSERT INTO "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" VALUES('p0','l00',NULL,NULL,'p0','s09',NULL,NULL,'{"lsk":{"S":"l00"},"pk":{"S":"p0"},"sk":{"S":"s09"}}');
CREATE TABLE "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" (
    pk TEXT NOT NULL,
    sk_s TEXT,
    sk_n TEXT,
    sk_b BLOB,
    base_pk TEXT NOT NULL,
    base_sk_s TEXT,
    base_sk_n TEXT,
    base_sk_b BLOB,
    item_data TEXT NOT NULL,
    PRIMARY KEY (pk, base_pk, base_sk_s)
);
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g0',NULL,'1',NULL,'p0','s00',NULL,NULL,'{"expires":{"N":"4102444800"},"gpk":{"S":"g0"},"gsk":{"N":"0"},"lsk":{"S":"l09"},"payload":{"S":"item 0"},"pk":{"S":"p0"},"sk":{"S":"s00"}}');
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g1',NULL,'21000011',NULL,'p1','s01',NULL,NULL,'{"expires":{"N":"4102444801"},"gpk":{"S":"g1"},"gsk":{"N":"1"},"lsk":{"S":"l08"},"payload":{"S":"item 1"},"pk":{"S":"p1"},"sk":{"S":"s01"}}');
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g0',NULL,'21000012',NULL,'p2','s02',NULL,NULL,'{"expires":{"N":"4102444802"},"gpk":{"S":"g0"},"gsk":{"N":"2"},"lsk":{"S":"l07"},"payload":{"S":"item 2"},"pk":{"S":"p2"},"sk":{"S":"s02"}}');
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g1',NULL,'21000013',NULL,'p0','s03',NULL,NULL,'{"expires":{"N":"4102444803"},"gpk":{"S":"g1"},"gsk":{"N":"3"},"lsk":{"S":"l06"},"payload":{"S":"item 3"},"pk":{"S":"p0"},"sk":{"S":"s03"}}');
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g0',NULL,'21000014',NULL,'p1','s04',NULL,NULL,'{"expires":{"N":"4102444804"},"gpk":{"S":"g0"},"gsk":{"N":"4"},"lsk":{"S":"l05"},"payload":{"S":"item 4"},"pk":{"S":"p1"},"sk":{"S":"s04"}}');
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g1',NULL,'21000015',NULL,'p2','s05',NULL,NULL,'{"expires":{"N":"4102444805"},"gpk":{"S":"g1"},"gsk":{"N":"5"},"lsk":{"S":"l04"},"payload":{"S":"item 5"},"pk":{"S":"p2"},"sk":{"S":"s05"}}');
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g1',NULL,'21000017',NULL,'p1','s07',NULL,NULL,'{"expires":{"N":"4102444807"},"gpk":{"S":"g1"},"gsk":{"N":"7"},"lsk":{"S":"l02"},"payload":{"S":"item 7"},"pk":{"S":"p1"},"sk":{"S":"s07"}}');
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g0',NULL,'21000016',NULL,'p0','s06',NULL,NULL,'{"expires":{"N":"4102444806"},"gpk":{"S":"g0"},"gsk":{"N":"6"},"lsk":{"S":"l03"},"payload":{"S":"item 6"},"pk":{"S":"p0"},"sk":{"S":"s06"}}');
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g0',NULL,'21000018',NULL,'p2','s08',NULL,NULL,'{"expires":{"N":"4102444808"},"gpk":{"S":"g0"},"gsk":{"N":"8"},"lsk":{"S":"l01"},"payload":{"S":"item 8"},"pk":{"S":"p2"},"sk":{"S":"s08"}}');
INSERT INTO "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" VALUES('g1',NULL,'21000019',NULL,'p0','s09',NULL,NULL,'{"expires":{"N":"4102444809"},"gpk":{"S":"g1"},"gsk":{"N":"9"},"lsk":{"S":"l00"},"payload":{"S":"item 9"},"pk":{"S":"p0"},"sk":{"S":"s09"}}');
CREATE TABLE access_keys (
    access_key_id TEXT PRIMARY KEY,
    secret_key_encrypted BLOB NOT NULL,
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (account_id, user_name)
        REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);
INSERT INTO "access_keys" VALUES('AKIAIOSFODNN7EXAMPLE',X'1866B25F86C0A4C57167553D604713BA06406D748459356A04ADF5AAE5CC4BBD0E86BB8E43A5F89DED75444B579E15DFB24BE8D9A71EE781F9A7342DB988D14C62AB9532','380366246314','dev',1,'2026-10-29T00:41:00.583Z');
CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
INSERT INTO "accounts" VALUES('380366246314','default','2026-10-29T00:41:00.337Z');
CREATE TABLE admin_users (
    admin_name TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
INSERT INTO "admin_users" VALUES('admin','$2b$12$4siKVOubqEVeH1r0X6S5K.GemKWvtMzD5wsKxmnaDl/I4SvpYWAyy','2026-10-29T00:41:00.570Z');
CREATE TABLE backup_items (
    backup_arn TEXT NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    pk TEXT NOT NULL,
    sk TEXT,
    item_data TEXT NOT NULL
);
CREATE TABLE backups (
    backup_arn TEXT PRIMARY KEY,
    backup_name TEXT NOT NULL,
    table_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    account_id TEXT NOT NULL,
    backup_status TEXT NOT NULL DEFAULT 'AVAILABLE',
    backup_type TEXT NOT NULL DEFAULT 'USER',
    backup_size_bytes INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER NOT NULL DEFAULT 0,
    key_schema TEXT NOT NULL,
    attribute_definitions TEXT NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput TEXT,
    stream_specification TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE continuous_backups (
    account_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pitr_enabled INTEGER NOT NULL DEFAULT 0,
    earliest_restorable TEXT,
    latest_restorable TEXT,
    PRIMARY KEY (account_id, table_name)
);
CREATE TABLE gsi_pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id TEXT NOT NULL,
    worker_partition INTEGER NOT NULL,
    old_item TEXT,
    new_item TEXT,
    index_context TEXT NOT NULL,
    ready_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE iam_group_members (
    account_id TEXT NOT NULL,
    group_name TEXT NOT NULL,
    user_name TEXT NOT NULL,
    PRIMARY KEY (account_id, group_name, user_name),
    FOREIGN KEY (account_id, group_name)
        REFERENCES iam_groups(account_id, group_name) ON DELETE CASCADE,
    FOREIGN KEY (account_id, user_name)
        REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);
CREATE TABLE iam_groups (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    group_name TEXT NOT NULL,
    group_arn TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, group_name)
);
CREATE TABLE iam_permissions_boundaries (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user', 'role')),
    principal_name TEXT NOT NULL,
    policy_document TEXT NOT NULL,
    PRIMARY KEY (account_id, principal_type, principal_name)
);
CREATE TABLE iam_policies (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user', 'group', 'role')),
    principal_name TEXT NOT NULL,
    policy_name TEXT NOT NULL,
    policy_document TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, principal_type, principal_name, policy_name)
);
INSERT INTO "iam_policies" VALUES('380366246314','user','dev','SelfServicePolicy','{"Statement":[{"Action":["iam:CreateAccessKey","iam:DeleteAccessKey","iam:ListAccessKeys","iam:ChangePassword"],"Effect":"Allow","Resource":"arn:aws:iam::380366246314:user/dev"}],"Version":"2012-10-17"}','2026-10-29T00:41:00.573Z');
CREATE TABLE iam_role_tags (
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, role_name, tag_key),
    FOREIGN KEY (account_id, role_name)
        REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
);
CREATE TABLE iam_roles (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    role_name TEXT NOT NULL,
    role_arn TEXT NOT NULL UNIQUE,
    trust_policy TEXT NOT NULL,
    permissions_boundary_arn TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, role_name)
);
CREATE TABLE iam_sessions (
    session_token TEXT PRIMARY KEY,
    access_key_id TEXT NOT NULL UNIQUE,
    secret_key_encrypted BLOB NOT NULL,
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    session_name TEXT NOT NULL,
    session_tags TEXT,
    session_policy TEXT,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (account_id, role_name)
        REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
);
CREATE TABLE iam_user_tags (
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, user_name, tag_key),
    FOREIGN KEY (account_id, user_name)
        REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);
CREATE TABLE iam_users (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    user_arn TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, user_name)
);
INSERT INTO "iam_users" VALUES('380366246314','dev','arn:aws:iam::380366246314:user/dev',NULL,'2026-10-29T00:41:00.573Z');
CREATE TABLE idempotency_tokens (
    account_id  TEXT NOT NULL,
    token       TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, token)
);
CREATE TABLE indexes (
    table_id TEXT NOT NULL,
    index_id TEXT NOT NULL,
    index_name TEXT NOT NULL,
    index_type TEXT NOT NULL,
    key_schema TEXT NOT NULL,
    projection TEXT NOT NULL,
    index_status TEXT NOT NULL DEFAULT 'ACTIVE',
    provisioned_throughput TEXT,
    propagation_delay_ms INTEGER,
    PRIMARY KEY (table_id, index_name),
    CONSTRAINT indexes_table_id_fkey
        FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE,
    CONSTRAINT chk_propagation_delay_ms_non_negative
        CHECK (propagation_delay_ms IS NULL OR propagation_delay_ms >= 0)
);
INSERT INTO "indexes" VALUES('25631d35-4f30-4da5-ae6a-4879d8eb79b6','fc40b212-8c5a-499b-9bb0-44bebe422a32','by_gpk','GSI','[{"AttributeName":"gpk","KeyType":"HASH"},{"AttributeName":"gsk","KeyType":"RANGE"}]','{"ProjectionType":"ALL"}','ACTIVE',NULL,NULL);
INSERT INTO "indexes" VALUES('25631d35-4f30-4da5-ae6a-4879d8eb79b6','457d180b-237e-4c52-b4f4-45c14c6373fa','by_lsk','LSI','[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"lsk","KeyType":"RANGE"}]','{"ProjectionType":"KEYS_ONLY"}','ACTIVE',NULL,NULL);
CREATE TABLE login_attempts (
    principal     TEXT NOT NULL,
    attempted_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    success       INTEGER NOT NULL,
    source_ip     TEXT
);
CREATE TABLE metrics (
    bucket TEXT NOT NULL,
    metric TEXT NOT NULL,
    table_name TEXT NOT NULL DEFAULT '',
    index_name TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    sum REAL NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    min REAL NOT NULL DEFAULT 9e999,
    max REAL NOT NULL DEFAULT -9e999,
    PRIMARY KEY (bucket, metric, table_name, index_name, operation)
);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','StorageQueryLatency','','Request','Data',28626.0,14,197.0,21037.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','StorageQueryLatency','','Request','Catalog',10971.0,5,255.0,8735.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ConsumedWriteCapacityUnits','cu_orders','','PutItem',10.0,10,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','UpdateTimeToLive',1.0,1,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','DescribeTable',1518.0,3,336.0,696.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','UpdateTimeToLive',956.0,1,956.0,956.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','Query',1110.0,3,278.0,498.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ConsumedReadCapacityUnits','cu_orders','by_lsk','Query',0.5,1,0.5,0.5);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ConsumedReadCapacityUnits','cu_orders','','Scan',0.5,1,0.5,0.5);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','DescribeTable',3.0,3,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ReturnedItemCount','cu_orders','','Query',12.0,3,3.0,5.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ConsumedReadCapacityUnits','cu_orders','by_gpk','Query',1.0,2,0.5,0.5);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ReturnedBytes','cu_orders','','Scan',508.0,1,508.0,508.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','PutItem',10.0,10,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','CreateTable',1.0,1,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','Scan',1.0,1,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','StorageQueryCount','','Request','Catalog',5.0,5,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','Scan',237.0,1,237.0,237.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','PutItem',28378.0,10,597.0,21064.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ReturnedBytes','cu_orders','','Query',464.0,3,60.0,253.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','StorageQueryCount','','Request','Data',14.0,14,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ReturnedItemCount','cu_orders','','Scan',10.0,1,10.0,10.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','Query',3.0,3,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','CreateTable',9554.0,1,9554.0,9554.0);
CREATE TABLE schema_history (
    filename TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE seq_counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
INSERT INTO "seq_counters" VALUES('stream',1793234460000010);
CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT INTO "settings" VALUES('catalog_version','0.0.3');
INSERT INTO "settings" VALUES('control_plane_delay_seconds','0.25');
INSERT INTO "settings" VALUES('index_propagation_delay_ms','10');
INSERT INTO "settings" VALUES('encryption_key','AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=');
INSERT INTO "settings" VALUES('default_account_id','380366246314');
CREATE TABLE stream_records (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    sequence_number TEXT NOT NULL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (shard_id, sequence_number)
);
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000001','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"8d4774c1-57b1-402c-929c-82f020d2243e","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p0"},"sk":{"S":"s00"}},"NewImage":{"expires":{"N":"4102444800"},"gpk":{"S":"g0"},"gsk":{"N":"0"},"lsk":{"S":"l09"},"payload":{"S":"item 0"},"pk":{"S":"p0"},"sk":{"S":"s00"}},"SequenceNumber":"000001793234460000001","SizeBytes":49,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.161Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000003','000001793234460000002','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"373ad8a0-19d9-4d13-848d-7e0b40456b20","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p1"},"sk":{"S":"s01"}},"NewImage":{"expires":{"N":"4102444801"},"gpk":{"S":"g1"},"gsk":{"N":"1"},"lsk":{"S":"l08"},"payload":{"S":"item 1"},"pk":{"S":"p1"},"sk":{"S":"s01"}},"SequenceNumber":"000001793234460000002","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.163Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000003','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"78f0b401-612b-4dcd-a661-05b748e90c5a","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p2"},"sk":{"S":"s02"}},"NewImage":{"expires":{"N":"4102444802"},"gpk":{"S":"g0"},"gsk":{"N":"2"},"lsk":{"S":"l07"},"payload":{"S":"item 2"},"pk":{"S":"p2"},"sk":{"S":"s02"}},"SequenceNumber":"000001793234460000003","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.165Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000004','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"b92c7ed5-c79e-4db3-9ca7-d200f6d3135c","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p0"},"sk":{"S":"s03"}},"NewImage":{"expires":{"N":"4102444803"},"gpk":{"S":"g1"},"gsk":{"N":"3"},"lsk":{"S":"l06"},"payload":{"S":"item 3"},"pk":{"S":"p0"},"sk":{"S":"s03"}},"SequenceNumber":"000001793234460000004","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.167Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000003','000001793234460000005','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"014045e0-c4ea-402e-83c1-f80c382a228c","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p1"},"sk":{"S":"s04"}},"NewImage":{"expires":{"N":"4102444804"},"gpk":{"S":"g0"},"gsk":{"N":"4"},"lsk":{"S":"l05"},"payload":{"S":"item 4"},"pk":{"S":"p1"},"sk":{"S":"s04"}},"SequenceNumber":"000001793234460000005","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.169Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000006','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"b465dc38-1097-451c-8c5c-7a292323079b","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p2"},"sk":{"S":"s05"}},"NewImage":{"expires":{"N":"4102444805"},"gpk":{"S":"g1"},"gsk":{"N":"5"},"lsk":{"S":"l04"},"payload":{"S":"item 5"},"pk":{"S":"p2"},"sk":{"S":"s05"}},"SequenceNumber":"000001793234460000006","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.171Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000007','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"68510cca-bd99-49f6-b778-4d3dd03382be","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p0"},"sk":{"S":"s06"}},"NewImage":{"expires":{"N":"4102444806"},"gpk":{"S":"g0"},"gsk":{"N":"6"},"lsk":{"S":"l03"},"payload":{"S":"item 6"},"pk":{"S":"p0"},"sk":{"S":"s06"}},"SequenceNumber":"000001793234460000007","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.193Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000003','000001793234460000008','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"81c6fae5-e4d5-427f-8d27-647ab9386f44","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p1"},"sk":{"S":"s07"}},"NewImage":{"expires":{"N":"4102444807"},"gpk":{"S":"g1"},"gsk":{"N":"7"},"lsk":{"S":"l02"},"payload":{"S":"item 7"},"pk":{"S":"p1"},"sk":{"S":"s07"}},"SequenceNumber":"000001793234460000008","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.195Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000009','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"8ba3687a-c903-45a5-ace2-72c0c85c1b8f","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p2"},"sk":{"S":"s08"}},"NewImage":{"expires":{"N":"4102444808"},"gpk":{"S":"g0"},"gsk":{"N":"8"},"lsk":{"S":"l01"},"payload":{"S":"item 8"},"pk":{"S":"p2"},"sk":{"S":"s08"}},"SequenceNumber":"000001793234460000009","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.197Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000010','25631d35-4f30-4da5-ae6a-4879d8eb79b6','Insert','{"eventID":"299c0428-4a36-4f3d-a168-0e6e11161d06","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p0"},"sk":{"S":"s09"}},"NewImage":{"expires":{"N":"4102444809"},"gpk":{"S":"g1"},"gsk":{"N":"9"},"lsk":{"S":"l00"},"payload":{"S":"item 9"},"pk":{"S":"p0"},"sk":{"S":"s09"}},"SequenceNumber":"000001793234460000010","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.199Z');
CREATE TABLE stream_shards (
    shard_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL REFERENCES tables(table_id) ON DELETE CASCADE,
    parent_shard_id TEXT,
    starting_sequence_number TEXT NOT NULL,
    ending_sequence_number TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
INSERT INTO "stream_shards" VALUES('shardId-cu_orders-0000000000000000','25631d35-4f30-4da5-ae6a-4879d8eb79b6',NULL,'000000000000000000000',NULL,'2026-10-29T00:41:01.126Z');
INSERT INTO "stream_shards" VALUES('shardId-cu_orders-0000000000000001','25631d35-4f30-4da5-ae6a-4879d8eb79b6',NULL,'000000000000000000000',NULL,'2026-10-29T00:41:01.126Z');
INSERT INTO "stream_shards" VALUES('shardId-cu_orders-0000000000000002','25631d35-4f30-4da5-ae6a-4879d8eb79b6',NULL,'000000000000000000000',NULL,'2026-10-29T00:41:01.126Z');
INSERT INTO "stream_shards" VALUES('shardId-cu_orders-0000000000000003','25631d35-4f30-4da5-ae6a-4879d8eb79b6',NULL,'000000000000000000000',NULL,'2026-10-29T00:41:01.126Z');
CREATE TABLE tables (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    key_schema TEXT NOT NULL,
    attribute_definitions TEXT NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput TEXT,
    stream_specification TEXT,
    table_status TEXT NOT NULL DEFAULT 'CREATING',
    creation_date_time TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    table_size_bytes INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER NOT NULL DEFAULT 0,
    table_arn TEXT NOT NULL,
    table_id TEXT NOT NULL,
    ttl_attribute TEXT,
    deletion_protection_enabled INTEGER NOT NULL DEFAULT 0,
    status_transition_at TEXT,
    stream_label TEXT,
    ttl_index_ready INTEGER NOT NULL DEFAULT 0,
    table_class TEXT,
    sse_specification TEXT,
    on_demand_throughput TEXT,
    PRIMARY KEY (account_id, table_name),
    CONSTRAINT tables_table_id_unique UNIQUE (table_id)
);
INSERT INTO "tables" VALUES('380366246314','cu_orders','[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]','[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"},{"AttributeName":"gpk","AttributeType":"S"},{"AttributeName":"gsk","AttributeType":"N"},{"AttributeName":"lsk","AttributeType":"S"}]','PAY_PER_REQUEST',NULL,'{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}','ACTIVE','2026-10-29T00:41:01.125133157Z',0,0,'arn:aws:dynamodb:us-east-1:380366246314:table/cu_orders','25631d35-4f30-4da5-ae6a-4879d8eb79b6','expires',0,NULL,'2026-10-29T00:41:01',1,NULL,NULL,NULL);
CREATE TABLE tags (
    resource_arn TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (resource_arn, tag_key)
);
CREATE TABLE vector_indexes (
    table_id TEXT NOT NULL,
    index_id TEXT NOT NULL,
    index_name TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    distance_function TEXT NOT NULL,
    vector_attribute TEXT NOT NULL,
    search_schema TEXT,
    projection TEXT NOT NULL,
    index_status TEXT NOT NULL DEFAULT 'CREATING',
    backfilling INTEGER,
    -- Items the backfill skipped because their stored bytes cannot enter the
    -- index (unparseable row, malformed or wrong-dimension vector). NULL until
    -- a backfill has completed; 0 afterwards when nothing was skipped. Kept so
    -- an operator can see that an ACTIVE index deliberately omits rows, rather
    -- than the build looping forever on them or dying part-way.
    skipped_item_count INTEGER,
    PRIMARY KEY (table_id, index_name),
    CONSTRAINT vector_indexes_table_id_fkey
        FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE,
    CONSTRAINT chk_vector_dimensions_positive CHECK (dimensions > 0),
    CONSTRAINT chk_vector_backfilling_bool
        CHECK (backfilling IS NULL OR backfilling IN (0, 1)),
    -- An ACTIVE index must not carry the member at all, which is what the
    -- service does. Enforced here as well as in core, so a bug in the backend
    -- cannot persist a state the wire contract forbids.
    CONSTRAINT chk_vector_active_has_no_backfilling
        CHECK (index_status <> 'ACTIVE' OR backfilling IS NULL)
);
CREATE INDEX idx_tables_pending_transition
    ON tables (status_transition_at)
    WHERE status_transition_at IS NOT NULL;
CREATE UNIQUE INDEX idx_vector_indexes_index_id
    ON vector_indexes (index_id);
CREATE INDEX idx_stream_shards_table ON stream_shards (table_id);
CREATE INDEX idx_stream_records_created ON stream_records (created_at);
CREATE INDEX idx_idempotency_tokens_created ON idempotency_tokens (created_at);
CREATE INDEX idx_metrics_bucket ON metrics (bucket);
CREATE INDEX idx_login_attempts_principal_time
    ON login_attempts (principal, attempted_at DESC);
CREATE INDEX idx_login_attempts_source_ip_time
    ON login_attempts (source_ip, attempted_at DESC)
    WHERE source_ip IS NOT NULL;
CREATE INDEX idx_backups_table ON backups (account_id, table_name);
CREATE INDEX idx_backup_items_arn ON backup_items (backup_arn);
CREATE INDEX idx_gsi_pending_claim
    ON gsi_pending (worker_partition, ready_at, id);
CREATE INDEX "idx_order_fc40b212-8c5a-499b-9bb0-44bebe422a32" ON "_ddb_fc40b212-8c5a-499b-9bb0-44bebe422a32" (pk, sk_n, base_pk, base_sk_s);
CREATE INDEX "idx_order_457d180b-237e-4c52-b4f4-45c14c6373fa" ON "_ddb_457d180b-237e-4c52-b4f4-45c14c6373fa" (pk, sk_s, base_pk, base_sk_s);
CREATE INDEX "idx_ttl_25631d35-4f30-4da5-ae6a-4879d8eb79b6" ON "_ddb_25631d35-4f30-4da5-ae6a-4879d8eb79b6" (CAST(json_extract(item_data, '$."expires".N') AS INTEGER)) WHERE json_extract(item_data, '$."expires".N') IS NOT NULL;
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('gsi_pending',10);
COMMIT;
