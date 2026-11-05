PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" (
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
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p0','l09',NULL,NULL,'p0','s00',NULL,NULL,'{"lsk":{"S":"l09"},"pk":{"S":"p0"},"sk":{"S":"s00"}}');
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p1','l08',NULL,NULL,'p1','s01',NULL,NULL,'{"lsk":{"S":"l08"},"pk":{"S":"p1"},"sk":{"S":"s01"}}');
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p2','l07',NULL,NULL,'p2','s02',NULL,NULL,'{"lsk":{"S":"l07"},"pk":{"S":"p2"},"sk":{"S":"s02"}}');
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p0','l06',NULL,NULL,'p0','s03',NULL,NULL,'{"lsk":{"S":"l06"},"pk":{"S":"p0"},"sk":{"S":"s03"}}');
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p1','l05',NULL,NULL,'p1','s04',NULL,NULL,'{"lsk":{"S":"l05"},"pk":{"S":"p1"},"sk":{"S":"s04"}}');
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p2','l04',NULL,NULL,'p2','s05',NULL,NULL,'{"lsk":{"S":"l04"},"pk":{"S":"p2"},"sk":{"S":"s05"}}');
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p0','l03',NULL,NULL,'p0','s06',NULL,NULL,'{"lsk":{"S":"l03"},"pk":{"S":"p0"},"sk":{"S":"s06"}}');
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p1','l02',NULL,NULL,'p1','s07',NULL,NULL,'{"lsk":{"S":"l02"},"pk":{"S":"p1"},"sk":{"S":"s07"}}');
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p2','l01',NULL,NULL,'p2','s08',NULL,NULL,'{"lsk":{"S":"l01"},"pk":{"S":"p2"},"sk":{"S":"s08"}}');
INSERT INTO "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" VALUES('p0','l00',NULL,NULL,'p0','s09',NULL,NULL,'{"lsk":{"S":"l00"},"pk":{"S":"p0"},"sk":{"S":"s09"}}');
CREATE TABLE "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" (
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
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g0',NULL,'1',NULL,'p0','s00',NULL,NULL,'{"expires":{"N":"4102444800"},"gpk":{"S":"g0"},"gsk":{"N":"0"},"lsk":{"S":"l09"},"payload":{"S":"item 0"},"pk":{"S":"p0"},"sk":{"S":"s00"}}');
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g1',NULL,'21000011',NULL,'p1','s01',NULL,NULL,'{"expires":{"N":"4102444801"},"gpk":{"S":"g1"},"gsk":{"N":"1"},"lsk":{"S":"l08"},"payload":{"S":"item 1"},"pk":{"S":"p1"},"sk":{"S":"s01"}}');
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g0',NULL,'21000012',NULL,'p2','s02',NULL,NULL,'{"expires":{"N":"4102444802"},"gpk":{"S":"g0"},"gsk":{"N":"2"},"lsk":{"S":"l07"},"payload":{"S":"item 2"},"pk":{"S":"p2"},"sk":{"S":"s02"}}');
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g1',NULL,'21000013',NULL,'p0','s03',NULL,NULL,'{"expires":{"N":"4102444803"},"gpk":{"S":"g1"},"gsk":{"N":"3"},"lsk":{"S":"l06"},"payload":{"S":"item 3"},"pk":{"S":"p0"},"sk":{"S":"s03"}}');
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g0',NULL,'21000014',NULL,'p1','s04',NULL,NULL,'{"expires":{"N":"4102444804"},"gpk":{"S":"g0"},"gsk":{"N":"4"},"lsk":{"S":"l05"},"payload":{"S":"item 4"},"pk":{"S":"p1"},"sk":{"S":"s04"}}');
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g1',NULL,'21000015',NULL,'p2','s05',NULL,NULL,'{"expires":{"N":"4102444805"},"gpk":{"S":"g1"},"gsk":{"N":"5"},"lsk":{"S":"l04"},"payload":{"S":"item 5"},"pk":{"S":"p2"},"sk":{"S":"s05"}}');
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g0',NULL,'21000016',NULL,'p0','s06',NULL,NULL,'{"expires":{"N":"4102444806"},"gpk":{"S":"g0"},"gsk":{"N":"6"},"lsk":{"S":"l03"},"payload":{"S":"item 6"},"pk":{"S":"p0"},"sk":{"S":"s06"}}');
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g1',NULL,'21000017',NULL,'p1','s07',NULL,NULL,'{"expires":{"N":"4102444807"},"gpk":{"S":"g1"},"gsk":{"N":"7"},"lsk":{"S":"l02"},"payload":{"S":"item 7"},"pk":{"S":"p1"},"sk":{"S":"s07"}}');
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g0',NULL,'21000018',NULL,'p2','s08',NULL,NULL,'{"expires":{"N":"4102444808"},"gpk":{"S":"g0"},"gsk":{"N":"8"},"lsk":{"S":"l01"},"payload":{"S":"item 8"},"pk":{"S":"p2"},"sk":{"S":"s08"}}');
INSERT INTO "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" VALUES('g1',NULL,'21000019',NULL,'p0','s09',NULL,NULL,'{"expires":{"N":"4102444809"},"gpk":{"S":"g1"},"gsk":{"N":"9"},"lsk":{"S":"l00"},"payload":{"S":"item 9"},"pk":{"S":"p0"},"sk":{"S":"s09"}}');
CREATE TABLE "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" (
    pk TEXT NOT NULL,
    sk_s TEXT,
    sk_n TEXT,
    sk_b BLOB,
    item_data TEXT NOT NULL,
    PRIMARY KEY (pk, sk_s)
);
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p0','s00',NULL,NULL,'{"expires":{"N":"4102444800"},"gpk":{"S":"g0"},"gsk":{"N":"0"},"lsk":{"S":"l09"},"payload":{"S":"item 0"},"pk":{"S":"p0"},"sk":{"S":"s00"}}');
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p1','s01',NULL,NULL,'{"expires":{"N":"4102444801"},"gpk":{"S":"g1"},"gsk":{"N":"1"},"lsk":{"S":"l08"},"payload":{"S":"item 1"},"pk":{"S":"p1"},"sk":{"S":"s01"}}');
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p2','s02',NULL,NULL,'{"expires":{"N":"4102444802"},"gpk":{"S":"g0"},"gsk":{"N":"2"},"lsk":{"S":"l07"},"payload":{"S":"item 2"},"pk":{"S":"p2"},"sk":{"S":"s02"}}');
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p0','s03',NULL,NULL,'{"expires":{"N":"4102444803"},"gpk":{"S":"g1"},"gsk":{"N":"3"},"lsk":{"S":"l06"},"payload":{"S":"item 3"},"pk":{"S":"p0"},"sk":{"S":"s03"}}');
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p1','s04',NULL,NULL,'{"expires":{"N":"4102444804"},"gpk":{"S":"g0"},"gsk":{"N":"4"},"lsk":{"S":"l05"},"payload":{"S":"item 4"},"pk":{"S":"p1"},"sk":{"S":"s04"}}');
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p2','s05',NULL,NULL,'{"expires":{"N":"4102444805"},"gpk":{"S":"g1"},"gsk":{"N":"5"},"lsk":{"S":"l04"},"payload":{"S":"item 5"},"pk":{"S":"p2"},"sk":{"S":"s05"}}');
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p0','s06',NULL,NULL,'{"expires":{"N":"4102444806"},"gpk":{"S":"g0"},"gsk":{"N":"6"},"lsk":{"S":"l03"},"payload":{"S":"item 6"},"pk":{"S":"p0"},"sk":{"S":"s06"}}');
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p1','s07',NULL,NULL,'{"expires":{"N":"4102444807"},"gpk":{"S":"g1"},"gsk":{"N":"7"},"lsk":{"S":"l02"},"payload":{"S":"item 7"},"pk":{"S":"p1"},"sk":{"S":"s07"}}');
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p2','s08',NULL,NULL,'{"expires":{"N":"4102444808"},"gpk":{"S":"g0"},"gsk":{"N":"8"},"lsk":{"S":"l01"},"payload":{"S":"item 8"},"pk":{"S":"p2"},"sk":{"S":"s08"}}');
INSERT INTO "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" VALUES('p0','s09',NULL,NULL,'{"expires":{"N":"4102444809"},"gpk":{"S":"g1"},"gsk":{"N":"9"},"lsk":{"S":"l00"},"payload":{"S":"item 9"},"pk":{"S":"p0"},"sk":{"S":"s09"}}');
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
INSERT INTO "access_keys" VALUES('AKIAIOSFODNN7EXAMPLE',X'E0595559CC9DCEC60E553ABCD6C3F6C6FE3749EC6238A1E3FC823C654F99F4E8D6F034A49958EA2FA4333CBD52B1E272153D92C20138D8EFC756D2942A81BEB753C66178','504105003262','dev',1,'2026-10-29T00:41:01.012Z');
CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
INSERT INTO "accounts" VALUES('504105003262','default','2026-10-29T00:41:00.749Z');
CREATE TABLE admin_users (
    admin_name TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
INSERT INTO "admin_users" VALUES('admin','$2b$12$yTvblHKpHs.xWDXk0hSW3egUPxGVvxXQ5LH5U9.5edt7THVfQ3c8O','2026-10-29T00:41:00.991Z');
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
INSERT INTO "iam_policies" VALUES('504105003262','user','dev','SelfServicePolicy','{"Statement":[{"Action":["iam:CreateAccessKey","iam:DeleteAccessKey","iam:ListAccessKeys","iam:ChangePassword"],"Effect":"Allow","Resource":"arn:aws:iam::504105003262:user/dev"}],"Version":"2012-10-17"}','2026-10-29T00:41:00.996Z');
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
INSERT INTO "iam_users" VALUES('504105003262','dev','arn:aws:iam::504105003262:user/dev',NULL,'2026-10-29T00:41:00.994Z');
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
INSERT INTO "indexes" VALUES('aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','1b7fae79-2959-4b69-adcd-bc4b790c140d','by_gpk','GSI','[{"AttributeName":"gpk","KeyType":"HASH"},{"AttributeName":"gsk","KeyType":"RANGE"}]','{"ProjectionType":"ALL"}','ACTIVE',NULL,NULL);
INSERT INTO "indexes" VALUES('aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','081d4835-c5c3-4dd5-81d7-94cae593aa14','by_lsk','LSI','[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"lsk","KeyType":"RANGE"}]','{"ProjectionType":"KEYS_ONLY"}','ACTIVE',NULL,NULL);
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
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','DescribeTable',1816.0,3,431.0,735.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','PutItem',10.0,10,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','StorageQueryLatency','','Request','Data',47816.0,14,186.0,16903.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','Query',3.0,3,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','StorageQueryCount','','Request','Data',14.0,14,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','CreateTable',2178.0,1,2178.0,2178.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ConsumedReadCapacityUnits','cu_orders','','Scan',0.5,1,0.5,0.5);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ConsumedReadCapacityUnits','cu_orders','by_lsk','Query',0.5,1,0.5,0.5);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','StorageQueryCount','','Request','Catalog',5.0,5,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','DescribeTable',3.0,3,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','Query',1179.0,3,348.0,468.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','CreateTable',1.0,1,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ConsumedWriteCapacityUnits','cu_orders','','PutItem',10.0,10,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ReturnedItemCount','cu_orders','','Query',13.0,3,4.0,5.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','UpdateTimeToLive',1189.0,1,1189.0,1189.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ReturnedItemCount','cu_orders','','Scan',10.0,1,10.0,10.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','UpdateTimeToLive',1.0,1,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ReturnedBytes','cu_orders','','Query',515.0,3,60.0,253.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','StorageQueryLatency','','Request','Catalog',4548.0,5,364.0,1774.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','PutItem',48067.0,10,647.0,16941.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','SuccessfulRequestLatency','cu_orders','','Scan',234.0,1,234.0,234.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','RequestCount','','','Scan',1.0,1,1.0,1.0);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ConsumedReadCapacityUnits','cu_orders','by_gpk','Query',1.0,2,0.5,0.5);
INSERT INTO "metrics" VALUES('2026-10-29T00:41:00Z','ReturnedBytes','cu_orders','','Scan',508.0,1,508.0,508.0);
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
INSERT INTO "settings" VALUES('default_account_id','504105003262');
CREATE TABLE stream_records (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    sequence_number TEXT NOT NULL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (shard_id, sequence_number)
);
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000001','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"13a30289-be11-4e14-b575-209fb196cacd","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p0"},"sk":{"S":"s00"}},"NewImage":{"expires":{"N":"4102444800"},"gpk":{"S":"g0"},"gsk":{"N":"0"},"lsk":{"S":"l09"},"payload":{"S":"item 0"},"pk":{"S":"p0"},"sk":{"S":"s00"}},"SequenceNumber":"000001793234460000001","SizeBytes":49,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.516Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000003','000001793234460000002','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"82a5f72c-3759-4cd0-9d24-3e343487b4d6","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p1"},"sk":{"S":"s01"}},"NewImage":{"expires":{"N":"4102444801"},"gpk":{"S":"g1"},"gsk":{"N":"1"},"lsk":{"S":"l08"},"payload":{"S":"item 1"},"pk":{"S":"p1"},"sk":{"S":"s01"}},"SequenceNumber":"000001793234460000002","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.525Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000003','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"6e15d310-c09d-4a42-9103-7d98fdb65b3d","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p2"},"sk":{"S":"s02"}},"NewImage":{"expires":{"N":"4102444802"},"gpk":{"S":"g0"},"gsk":{"N":"2"},"lsk":{"S":"l07"},"payload":{"S":"item 2"},"pk":{"S":"p2"},"sk":{"S":"s02"}},"SequenceNumber":"000001793234460000003","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.527Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000004','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"5bccd10d-a61d-480d-9cda-fcf02e1f7b2a","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p0"},"sk":{"S":"s03"}},"NewImage":{"expires":{"N":"4102444803"},"gpk":{"S":"g1"},"gsk":{"N":"3"},"lsk":{"S":"l06"},"payload":{"S":"item 3"},"pk":{"S":"p0"},"sk":{"S":"s03"}},"SequenceNumber":"000001793234460000004","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.545Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000003','000001793234460000005','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"ab50bc7f-10c8-42a8-b32b-48fdc12ae42a","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p1"},"sk":{"S":"s04"}},"NewImage":{"expires":{"N":"4102444804"},"gpk":{"S":"g0"},"gsk":{"N":"4"},"lsk":{"S":"l05"},"payload":{"S":"item 4"},"pk":{"S":"p1"},"sk":{"S":"s04"}},"SequenceNumber":"000001793234460000005","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.554Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000006','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"6f65a1e5-b9ee-4af2-8ac2-c44cb4f23981","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p2"},"sk":{"S":"s05"}},"NewImage":{"expires":{"N":"4102444805"},"gpk":{"S":"g1"},"gsk":{"N":"5"},"lsk":{"S":"l04"},"payload":{"S":"item 5"},"pk":{"S":"p2"},"sk":{"S":"s05"}},"SequenceNumber":"000001793234460000006","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.556Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000007','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"67289088-af5c-4220-9615-ac08c608af4b","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p0"},"sk":{"S":"s06"}},"NewImage":{"expires":{"N":"4102444806"},"gpk":{"S":"g0"},"gsk":{"N":"6"},"lsk":{"S":"l03"},"payload":{"S":"item 6"},"pk":{"S":"p0"},"sk":{"S":"s06"}},"SequenceNumber":"000001793234460000007","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.558Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000003','000001793234460000008','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"63686c46-8907-4e18-8e9b-fb77ad458117","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p1"},"sk":{"S":"s07"}},"NewImage":{"expires":{"N":"4102444807"},"gpk":{"S":"g1"},"gsk":{"N":"7"},"lsk":{"S":"l02"},"payload":{"S":"item 7"},"pk":{"S":"p1"},"sk":{"S":"s07"}},"SequenceNumber":"000001793234460000008","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.569Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000009','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"f3991d77-142b-4b80-b4cc-060eeed6ca29","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p2"},"sk":{"S":"s08"}},"NewImage":{"expires":{"N":"4102444808"},"gpk":{"S":"g0"},"gsk":{"N":"8"},"lsk":{"S":"l01"},"payload":{"S":"item 8"},"pk":{"S":"p2"},"sk":{"S":"s08"}},"SequenceNumber":"000001793234460000009","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.571Z');
INSERT INTO "stream_records" VALUES('shardId-cu_orders-0000000000000001','000001793234460000010','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','Insert','{"eventID":"f77c3b0e-da1e-481b-97d3-9ab573d10b0d","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb","awsRegion":"us-east-1","dynamodb":{"ApproximateCreationDateTime":1793234462,"Keys":{"pk":{"S":"p0"},"sk":{"S":"s09"}},"NewImage":{"expires":{"N":"4102444809"},"gpk":{"S":"g1"},"gsk":{"N":"9"},"lsk":{"S":"l00"},"payload":{"S":"item 9"},"pk":{"S":"p0"},"sk":{"S":"s09"}},"SequenceNumber":"000001793234460000010","SizeBytes":51,"StreamViewType":"NEW_AND_OLD_IMAGES"}}','2026-10-29T00:41:02.573Z');
CREATE TABLE stream_shards (
    shard_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL REFERENCES tables(table_id) ON DELETE CASCADE,
    parent_shard_id TEXT,
    starting_sequence_number TEXT NOT NULL,
    ending_sequence_number TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
INSERT INTO "stream_shards" VALUES('shardId-cu_orders-0000000000000000','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec',NULL,'000000000000000000000',NULL,'2026-10-29T00:41:01.498Z');
INSERT INTO "stream_shards" VALUES('shardId-cu_orders-0000000000000001','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec',NULL,'000000000000000000000',NULL,'2026-10-29T00:41:01.498Z');
INSERT INTO "stream_shards" VALUES('shardId-cu_orders-0000000000000002','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec',NULL,'000000000000000000000',NULL,'2026-10-29T00:41:01.498Z');
INSERT INTO "stream_shards" VALUES('shardId-cu_orders-0000000000000003','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec',NULL,'000000000000000000000',NULL,'2026-10-29T00:41:01.498Z');
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
INSERT INTO "tables" VALUES('504105003262','cu_orders','[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]','[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"},{"AttributeName":"gpk","AttributeType":"S"},{"AttributeName":"gsk","AttributeType":"N"},{"AttributeName":"lsk","AttributeType":"S"}]','PAY_PER_REQUEST',NULL,'{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}','ACTIVE','2026-10-29T00:41:01.497730743Z',0,0,'arn:aws:dynamodb:us-east-1:504105003262:table/cu_orders','aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec','expires',0,NULL,'2026-10-29T00:41:01',1,NULL,NULL,NULL);
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
CREATE INDEX "idx_order_1b7fae79-2959-4b69-adcd-bc4b790c140d" ON "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" (pk, sk_n, base_pk, base_sk_s);
CREATE INDEX "idx_base_key_1b7fae79-2959-4b69-adcd-bc4b790c140d" ON "_ddb_1b7fae79-2959-4b69-adcd-bc4b790c140d" (base_pk, base_sk_s);
CREATE INDEX "idx_order_081d4835-c5c3-4dd5-81d7-94cae593aa14" ON "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" (pk, sk_s, base_pk, base_sk_s);
CREATE INDEX "idx_base_key_081d4835-c5c3-4dd5-81d7-94cae593aa14" ON "_ddb_081d4835-c5c3-4dd5-81d7-94cae593aa14" (base_pk, base_sk_s);
CREATE INDEX "idx_ttl_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" ON "_ddb_aa560d78-7f1f-4fcf-ba52-1b3e8e8031ec" (CAST(json_extract(item_data, '$."expires".N') AS INTEGER)) WHERE json_extract(item_data, '$."expires".N') IS NOT NULL;
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('gsi_pending',10);
COMMIT;
