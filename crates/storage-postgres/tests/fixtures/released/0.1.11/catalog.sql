--
-- PostgreSQL database dump
--


-- Dumped from database version 16.10 (Debian 16.10-1.pgdg12+1)
-- Dumped by pg_dump version 16.10 (Debian 16.10-1.pgdg12+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: access_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.access_keys (
    access_key_id text NOT NULL,
    secret_key_encrypted bytea NOT NULL,
    account_id text NOT NULL,
    user_name text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: accounts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.accounts (
    account_id text NOT NULL,
    account_name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: admin_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.admin_users (
    admin_name text NOT NULL,
    password_hash text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: backup_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backup_items (
    backup_arn text NOT NULL,
    pk text NOT NULL,
    sk text,
    item_data jsonb NOT NULL
);


--
-- Name: backups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backups (
    backup_arn text NOT NULL,
    backup_name text NOT NULL,
    table_id text NOT NULL,
    table_name text NOT NULL,
    account_id text NOT NULL,
    backup_status text DEFAULT 'AVAILABLE'::text NOT NULL,
    backup_type text DEFAULT 'USER'::text NOT NULL,
    backup_size_bytes bigint DEFAULT 0 NOT NULL,
    item_count bigint DEFAULT 0 NOT NULL,
    key_schema jsonb NOT NULL,
    attribute_definitions jsonb NOT NULL,
    billing_mode text DEFAULT 'PAY_PER_REQUEST'::text NOT NULL,
    provisioned_throughput jsonb,
    stream_specification jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    vector_indexes jsonb
);


--
-- Name: continuous_backups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.continuous_backups (
    account_id text NOT NULL,
    table_name text NOT NULL,
    pitr_enabled boolean DEFAULT false NOT NULL,
    earliest_restorable timestamp with time zone,
    latest_restorable timestamp with time zone
);


--
-- Name: iam_group_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.iam_group_members (
    account_id text NOT NULL,
    group_name text NOT NULL,
    user_name text NOT NULL
);


--
-- Name: iam_groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.iam_groups (
    account_id text NOT NULL,
    group_name text NOT NULL,
    group_arn text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: iam_permissions_boundaries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.iam_permissions_boundaries (
    account_id text NOT NULL,
    principal_type text NOT NULL,
    principal_name text NOT NULL,
    policy_document jsonb NOT NULL,
    CONSTRAINT iam_permissions_boundaries_principal_type_check CHECK ((principal_type = ANY (ARRAY['user'::text, 'role'::text])))
);


--
-- Name: iam_policies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.iam_policies (
    account_id text NOT NULL,
    principal_type text NOT NULL,
    principal_name text NOT NULL,
    policy_name text NOT NULL,
    policy_document jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT iam_policies_principal_type_check CHECK ((principal_type = ANY (ARRAY['user'::text, 'group'::text, 'role'::text])))
);


--
-- Name: iam_role_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.iam_role_tags (
    account_id text NOT NULL,
    role_name text NOT NULL,
    tag_key text NOT NULL,
    tag_value text NOT NULL
);


--
-- Name: iam_roles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.iam_roles (
    account_id text NOT NULL,
    role_name text NOT NULL,
    role_arn text NOT NULL,
    trust_policy jsonb NOT NULL,
    permissions_boundary_arn text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: iam_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.iam_sessions (
    session_token text NOT NULL,
    access_key_id text NOT NULL,
    secret_key_encrypted bytea NOT NULL,
    account_id text NOT NULL,
    role_name text NOT NULL,
    session_name text NOT NULL,
    session_tags jsonb,
    session_policy jsonb,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: iam_user_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.iam_user_tags (
    account_id text NOT NULL,
    user_name text NOT NULL,
    tag_key text NOT NULL,
    tag_value text NOT NULL
);


--
-- Name: iam_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.iam_users (
    account_id text NOT NULL,
    user_name text NOT NULL,
    user_arn text NOT NULL,
    password_hash text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: idempotency_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.idempotency_tokens (
    token text NOT NULL,
    fingerprint text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: indexes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.indexes (
    table_id text NOT NULL,
    index_id text DEFAULT (gen_random_uuid())::text NOT NULL,
    index_name text NOT NULL,
    index_type text NOT NULL,
    key_schema jsonb NOT NULL,
    projection jsonb NOT NULL,
    index_status text DEFAULT 'ACTIVE'::text NOT NULL,
    provisioned_throughput jsonb,
    propagation_delay_ms integer,
    CONSTRAINT chk_propagation_delay_ms_non_negative CHECK (((propagation_delay_ms IS NULL) OR (propagation_delay_ms >= 0)))
);


--
-- Name: login_attempts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.login_attempts (
    principal text NOT NULL,
    attempted_at timestamp with time zone DEFAULT now() NOT NULL,
    success boolean NOT NULL,
    source_ip text
);


--
-- Name: metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metrics (
    bucket timestamp with time zone NOT NULL,
    metric text NOT NULL,
    table_name text DEFAULT ''::text NOT NULL,
    index_name text DEFAULT ''::text NOT NULL,
    operation text DEFAULT ''::text NOT NULL,
    sum double precision DEFAULT 0 NOT NULL,
    count bigint DEFAULT 0 NOT NULL,
    min double precision DEFAULT 'Infinity'::double precision NOT NULL,
    max double precision DEFAULT '-Infinity'::double precision NOT NULL
);


--
-- Name: schema_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_history (
    filename text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.settings (
    key text NOT NULL,
    value text NOT NULL
);


--
-- Name: stream_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stream_records (
    shard_id text NOT NULL,
    sequence_number text NOT NULL,
    table_id text NOT NULL,
    event_name text NOT NULL,
    record_data jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stream_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stream_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stream_shards; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stream_shards (
    shard_id text NOT NULL,
    table_id text NOT NULL,
    parent_shard_id text,
    starting_sequence_number text NOT NULL,
    ending_sequence_number text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: tables; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tables (
    account_id text NOT NULL,
    table_name text NOT NULL,
    key_schema jsonb NOT NULL,
    attribute_definitions jsonb NOT NULL,
    billing_mode text DEFAULT 'PAY_PER_REQUEST'::text NOT NULL,
    provisioned_throughput jsonb,
    stream_specification jsonb,
    table_status text DEFAULT 'CREATING'::text NOT NULL,
    creation_date_time timestamp with time zone DEFAULT now() NOT NULL,
    table_size_bytes bigint DEFAULT 0 NOT NULL,
    item_count bigint DEFAULT 0 NOT NULL,
    table_arn text NOT NULL,
    table_id text NOT NULL,
    ttl_attribute text,
    deletion_protection_enabled boolean DEFAULT false NOT NULL,
    status_transition_at timestamp with time zone,
    stream_label text,
    ttl_index_ready boolean DEFAULT false NOT NULL,
    table_class text,
    sse_specification jsonb,
    on_demand_throughput jsonb
);


--
-- Name: tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tags (
    resource_arn text NOT NULL,
    tag_key text NOT NULL,
    tag_value text NOT NULL
);


--
-- Name: vector_indexes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vector_indexes (
    table_id text NOT NULL,
    index_id text NOT NULL,
    index_name text NOT NULL,
    dimensions integer NOT NULL,
    distance_function text NOT NULL,
    vector_attribute jsonb NOT NULL,
    search_schema jsonb,
    projection jsonb NOT NULL,
    index_status text DEFAULT 'CREATING'::text NOT NULL,
    backfilling boolean,
    skipped_item_count bigint,
    build_owner text,
    build_heartbeat_at timestamp with time zone,
    CONSTRAINT chk_vector_active_has_no_backfilling CHECK (((index_status <> 'ACTIVE'::text) OR (backfilling IS NULL))),
    CONSTRAINT chk_vector_dimensions_positive CHECK ((dimensions > 0))
);


--
-- Data for Name: access_keys; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.access_keys VALUES ('AKIAEXTENDDBKSFBXT8F', '\xdcef82f5b59064ff5779777f88c56d37c47ee334f3998bedfa85a590b16863391bdbd7795123ae80f6bd6039ce3f454caddac473e15ff1b0a20e93d30183399ad31720c5', '123456789012', 'tester', true, '2026-10-29 07:19:59.856469+00');


--
-- Data for Name: accounts; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.accounts VALUES ('611005726111', 'default', '2026-10-29 07:19:53.07403+00');
INSERT INTO public.accounts VALUES ('123456789012', 'seed', '2026-10-29 07:19:58.901185+00');


--
-- Data for Name: admin_users; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.admin_users VALUES ('admin', '$2b$04$KQg8SAyQ3g2v0nmyz2wPXOWb2pGZtfJ0yLe6E8V1OAQmvQjQlZ9ka', '2026-10-29 07:19:53.313438+00');


--
-- Data for Name: backup_items; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p0', NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p1', NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p2', NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p0', NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p1', NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p2', NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p0', NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p1', NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p2', NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public.backup_items VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed-p0', NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: backups; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.backups VALUES ('arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl/backup/1793258403285-0300e58a', 'seed_backup', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'seed_tbl', '123456789012', 'AVAILABLE', 'USER', 0, 10, '[{"KeyType": "HASH", "AttributeName": "pk"}, {"KeyType": "RANGE", "AttributeName": "sk"}]', '[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}, {"AttributeName": "gpk", "AttributeType": "S"}, {"AttributeName": "lsk", "AttributeType": "N"}]', 'PAY_PER_REQUEST', NULL, NULL, '2026-10-29 07:20:03.290884+00', '{"Version": 1, "VectorIndexes": []}');


--
-- Data for Name: continuous_backups; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: iam_group_members; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: iam_groups; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: iam_permissions_boundaries; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: iam_policies; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.iam_policies VALUES ('123456789012', 'user', 'tester', 'SelfServicePolicy', '{"Version": "2012-10-17", "Statement": [{"Action": ["iam:CreateAccessKey", "iam:DeleteAccessKey", "iam:ListAccessKeys", "iam:ChangePassword"], "Effect": "Allow", "Resource": "arn:aws:iam::123456789012:user/tester"}]}', '2026-10-29 07:19:59.219895+00');
INSERT INTO public.iam_policies VALUES ('123456789012', 'user', 'tester', 'ddb', '{"Version": "2012-10-17", "Statement": [{"Action": "dynamodb:*", "Effect": "Allow", "Resource": "*"}]}', '2026-10-29 07:19:59.539457+00');


--
-- Data for Name: iam_role_tags; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: iam_roles; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: iam_sessions; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: iam_user_tags; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: iam_users; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.iam_users VALUES ('123456789012', 'tester', 'arn:aws:iam::123456789012:user/tester', NULL, '2026-10-29 07:19:59.219895+00');


--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: indexes; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.indexes VALUES ('f98cd4da-fd59-4d78-b38b-074badab0471', '02d624d6-cbe9-47b1-b29a-660e55b59e95', 'gsi1', 'GSI', '[{"KeyType": "HASH", "AttributeName": "gpk"}, {"KeyType": "RANGE", "AttributeName": "sk"}]', '{"ProjectionType": "ALL"}', 'ACTIVE', NULL, NULL);
INSERT INTO public.indexes VALUES ('f98cd4da-fd59-4d78-b38b-074badab0471', 'c6f40bd3-a950-472d-825d-012dfa26de08', 'lsi1', 'LSI', '[{"KeyType": "HASH", "AttributeName": "pk"}, {"KeyType": "RANGE", "AttributeName": "lsk"}]', '{"ProjectionType": "ALL"}', 'ACTIVE', NULL, NULL);


--
-- Data for Name: login_attempts; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: metrics; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'RequestCount', '', '', 'CreateBackup', 1, 1, 1, 1);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'RequestCount', '', '', 'UpdateTimeToLive', 1, 1, 1, 1);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'ConsumedWriteCapacityUnits', 'seed_simple', '', 'PutItem', 3, 3, 1, 1);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'StorageQueryLatency', '', 'Request', 'Data', 46379, 14, 1435, 9307);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'PoolIdleConnections', '', '', '', 9, 1, 9, 9);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'RequestCount', '', '', 'DescribeTable', 6, 6, 1, 1);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'SuccessfulRequestLatency', 'seed_tbl', '', 'PutItem', 33200, 10, 2144, 5390);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'SuccessfulRequestLatency', 'seed_simple', '', 'DescribeTable', 2527, 3, 716, 947);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'PoolActiveConnections', '', '', '', 0, 1, 0, 0);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'SuccessfulRequestLatency', 'seed_simple', '', 'PutItem', 5561, 3, 1476, 2138);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'SuccessfulRequestLatency', 'seed_tbl', '', 'DescribeTable', 3328, 3, 651, 1421);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'StorageQueryLatency', '', 'Request', 'Catalog', 76123, 9, 463, 49768);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'SuccessfulRequestLatency', 'seed_tbl', '', 'CreateTable', 56614, 1, 56614, 56614);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'ConsumedWriteCapacityUnits', 'seed_tbl', '', 'PutItem', 10, 10, 1, 1);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'RequestCount', '', '', 'CreateTable', 2, 2, 1, 1);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'StorageQueryCount', '', 'Request', 'Catalog', 9, 9, 1, 1);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'SuccessfulRequestLatency', 'seed_simple', '', 'CreateTable', 11770, 1, 11770, 11770);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'RequestCount', '', '', 'PutItem', 13, 13, 1, 1);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'SuccessfulRequestLatency', 'seed_tbl', '', 'CreateBackup', 9440, 1, 9440, 9440);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'StorageQueryCount', '', 'Request', 'Data', 14, 14, 1, 1);
INSERT INTO public.metrics VALUES ('2026-10-29 07:20:00+00', 'SuccessfulRequestLatency', 'seed_tbl', '', 'UpdateTimeToLive', 10821, 1, 10821, 10821);


--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_schema.sql', '2026-10-29 07:19:52.932166+00');
INSERT INTO public.schema_history VALUES ('002_vector_indexes.sql', '2026-10-29 07:19:52.946842+00');


--
-- Data for Name: settings; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.settings VALUES ('control_plane_delay_seconds', '0.25');
INSERT INTO public.settings VALUES ('index_propagation_delay_ms', '10');
INSERT INTO public.settings VALUES ('catalog_version', '0.0.3');
INSERT INTO public.settings VALUES ('data_database_connection_string', 'postgresql://extenddb_app:fixture@fixture-host:5432/extenddb');
INSERT INTO public.settings VALUES ('data_database_name', 'extenddb');
INSERT INTO public.settings VALUES ('encryption_key', 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=');
INSERT INTO public.settings VALUES ('default_account_id', '611005726111');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: tables; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.tables VALUES ('123456789012', 'seed_tbl', '[{"KeyType": "HASH", "AttributeName": "pk"}, {"KeyType": "RANGE", "AttributeName": "sk"}]', '[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}, {"AttributeName": "gpk", "AttributeType": "S"}, {"AttributeName": "lsk", "AttributeType": "N"}]', 'PAY_PER_REQUEST', NULL, '{"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}', 'ACTIVE', '2026-10-29 07:20:00.161915+00', 0, 0, 'arn:aws:dynamodb:us-east-1:123456789012:table/seed_tbl', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'exp', false, NULL, '2026-10-29T07:20:00', true, NULL, NULL, NULL);
INSERT INTO public.tables VALUES ('123456789012', 'seed_simple', '[{"KeyType": "HASH", "AttributeName": "id"}]', '[{"AttributeName": "id", "AttributeType": "N"}]', 'PAY_PER_REQUEST', NULL, NULL, 'ACTIVE', '2026-10-29 07:20:03.296283+00', 0, 0, 'arn:aws:dynamodb:us-east-1:123456789012:table/seed_simple', '98a10bcc-94ea-46c4-8659-e924ad1b28f6', NULL, false, NULL, NULL, false, NULL, NULL, NULL);


--
-- Data for Name: tags; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vector_indexes; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Name: stream_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.stream_seq', 1793258392703726, true);


--
-- Name: access_keys access_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_keys
    ADD CONSTRAINT access_keys_pkey PRIMARY KEY (access_key_id);


--
-- Name: accounts accounts_account_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_account_name_key UNIQUE (account_name);


--
-- Name: accounts accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (account_id);


--
-- Name: admin_users admin_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_users
    ADD CONSTRAINT admin_users_pkey PRIMARY KEY (admin_name);


--
-- Name: backups backups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backups
    ADD CONSTRAINT backups_pkey PRIMARY KEY (backup_arn);


--
-- Name: continuous_backups continuous_backups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.continuous_backups
    ADD CONSTRAINT continuous_backups_pkey PRIMARY KEY (account_id, table_name);


--
-- Name: iam_group_members iam_group_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_group_members
    ADD CONSTRAINT iam_group_members_pkey PRIMARY KEY (account_id, group_name, user_name);


--
-- Name: iam_groups iam_groups_group_arn_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_groups
    ADD CONSTRAINT iam_groups_group_arn_key UNIQUE (group_arn);


--
-- Name: iam_groups iam_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_groups
    ADD CONSTRAINT iam_groups_pkey PRIMARY KEY (account_id, group_name);


--
-- Name: iam_permissions_boundaries iam_permissions_boundaries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_permissions_boundaries
    ADD CONSTRAINT iam_permissions_boundaries_pkey PRIMARY KEY (account_id, principal_type, principal_name);


--
-- Name: iam_policies iam_policies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_policies
    ADD CONSTRAINT iam_policies_pkey PRIMARY KEY (account_id, principal_type, principal_name, policy_name);


--
-- Name: iam_role_tags iam_role_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_role_tags
    ADD CONSTRAINT iam_role_tags_pkey PRIMARY KEY (account_id, role_name, tag_key);


--
-- Name: iam_roles iam_roles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_roles
    ADD CONSTRAINT iam_roles_pkey PRIMARY KEY (account_id, role_name);


--
-- Name: iam_roles iam_roles_role_arn_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_roles
    ADD CONSTRAINT iam_roles_role_arn_key UNIQUE (role_arn);


--
-- Name: iam_sessions iam_sessions_access_key_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_sessions
    ADD CONSTRAINT iam_sessions_access_key_id_key UNIQUE (access_key_id);


--
-- Name: iam_sessions iam_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_sessions
    ADD CONSTRAINT iam_sessions_pkey PRIMARY KEY (session_token);


--
-- Name: iam_user_tags iam_user_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_user_tags
    ADD CONSTRAINT iam_user_tags_pkey PRIMARY KEY (account_id, user_name, tag_key);


--
-- Name: iam_users iam_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_users
    ADD CONSTRAINT iam_users_pkey PRIMARY KEY (account_id, user_name);


--
-- Name: iam_users iam_users_user_arn_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_users
    ADD CONSTRAINT iam_users_user_arn_key UNIQUE (user_arn);


--
-- Name: idempotency_tokens idempotency_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.idempotency_tokens
    ADD CONSTRAINT idempotency_tokens_pkey PRIMARY KEY (token);


--
-- Name: indexes indexes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.indexes
    ADD CONSTRAINT indexes_pkey PRIMARY KEY (table_id, index_name);


--
-- Name: metrics metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metrics
    ADD CONSTRAINT metrics_pkey PRIMARY KEY (bucket, metric, table_name, index_name, operation);


--
-- Name: schema_history schema_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_history
    ADD CONSTRAINT schema_history_pkey PRIMARY KEY (filename);


--
-- Name: settings settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.settings
    ADD CONSTRAINT settings_pkey PRIMARY KEY (key);


--
-- Name: stream_records stream_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_records
    ADD CONSTRAINT stream_records_pkey PRIMARY KEY (shard_id, sequence_number);


--
-- Name: stream_shards stream_shards_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_shards
    ADD CONSTRAINT stream_shards_pkey PRIMARY KEY (shard_id);


--
-- Name: tables tables_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tables
    ADD CONSTRAINT tables_pkey PRIMARY KEY (account_id, table_name);


--
-- Name: tables tables_table_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tables
    ADD CONSTRAINT tables_table_id_unique UNIQUE (table_id);


--
-- Name: tags tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_pkey PRIMARY KEY (resource_arn, tag_key);


--
-- Name: vector_indexes vector_indexes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vector_indexes
    ADD CONSTRAINT vector_indexes_pkey PRIMARY KEY (table_id, index_name);


--
-- Name: idx_backup_items_arn; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_backup_items_arn ON public.backup_items USING btree (backup_arn);


--
-- Name: idx_backups_table; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_backups_table ON public.backups USING btree (account_id, table_name);


--
-- Name: idx_idempotency_tokens_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_idempotency_tokens_created ON public.idempotency_tokens USING btree (created_at);


--
-- Name: idx_login_attempts_principal_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_login_attempts_principal_time ON public.login_attempts USING btree (principal, attempted_at DESC);


--
-- Name: idx_login_attempts_source_ip_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_login_attempts_source_ip_time ON public.login_attempts USING btree (source_ip, attempted_at DESC) WHERE (source_ip IS NOT NULL);


--
-- Name: idx_metrics_bucket; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_metrics_bucket ON public.metrics USING btree (bucket);


--
-- Name: idx_stream_records_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_records_created ON public.stream_records USING btree (created_at);


--
-- Name: idx_stream_shards_table; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_shards_table ON public.stream_shards USING btree (table_id);


--
-- Name: idx_tables_pending_transition; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tables_pending_transition ON public.tables USING btree (status_transition_at) WHERE (status_transition_at IS NOT NULL);


--
-- Name: idx_vector_indexes_index_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_vector_indexes_index_id ON public.vector_indexes USING btree (index_id);


--
-- Name: access_keys access_keys_account_id_user_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_keys
    ADD CONSTRAINT access_keys_account_id_user_name_fkey FOREIGN KEY (account_id, user_name) REFERENCES public.iam_users(account_id, user_name) ON DELETE CASCADE;


--
-- Name: backup_items backup_items_backup_arn_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backup_items
    ADD CONSTRAINT backup_items_backup_arn_fkey FOREIGN KEY (backup_arn) REFERENCES public.backups(backup_arn) ON DELETE CASCADE;


--
-- Name: iam_group_members iam_group_members_account_id_group_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_group_members
    ADD CONSTRAINT iam_group_members_account_id_group_name_fkey FOREIGN KEY (account_id, group_name) REFERENCES public.iam_groups(account_id, group_name) ON DELETE CASCADE;


--
-- Name: iam_group_members iam_group_members_account_id_user_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_group_members
    ADD CONSTRAINT iam_group_members_account_id_user_name_fkey FOREIGN KEY (account_id, user_name) REFERENCES public.iam_users(account_id, user_name) ON DELETE CASCADE;


--
-- Name: iam_groups iam_groups_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_groups
    ADD CONSTRAINT iam_groups_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.accounts(account_id) ON DELETE CASCADE;


--
-- Name: iam_permissions_boundaries iam_permissions_boundaries_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_permissions_boundaries
    ADD CONSTRAINT iam_permissions_boundaries_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.accounts(account_id) ON DELETE CASCADE;


--
-- Name: iam_policies iam_policies_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_policies
    ADD CONSTRAINT iam_policies_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.accounts(account_id) ON DELETE CASCADE;


--
-- Name: iam_role_tags iam_role_tags_account_id_role_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_role_tags
    ADD CONSTRAINT iam_role_tags_account_id_role_name_fkey FOREIGN KEY (account_id, role_name) REFERENCES public.iam_roles(account_id, role_name) ON DELETE CASCADE;


--
-- Name: iam_roles iam_roles_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_roles
    ADD CONSTRAINT iam_roles_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.accounts(account_id) ON DELETE CASCADE;


--
-- Name: iam_sessions iam_sessions_account_id_role_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_sessions
    ADD CONSTRAINT iam_sessions_account_id_role_name_fkey FOREIGN KEY (account_id, role_name) REFERENCES public.iam_roles(account_id, role_name) ON DELETE CASCADE;


--
-- Name: iam_user_tags iam_user_tags_account_id_user_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_user_tags
    ADD CONSTRAINT iam_user_tags_account_id_user_name_fkey FOREIGN KEY (account_id, user_name) REFERENCES public.iam_users(account_id, user_name) ON DELETE CASCADE;


--
-- Name: iam_users iam_users_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.iam_users
    ADD CONSTRAINT iam_users_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.accounts(account_id) ON DELETE CASCADE;


--
-- Name: indexes indexes_table_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.indexes
    ADD CONSTRAINT indexes_table_id_fkey FOREIGN KEY (table_id) REFERENCES public.tables(table_id) ON DELETE CASCADE;


--
-- Name: stream_records stream_records_shard_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_records
    ADD CONSTRAINT stream_records_shard_id_fkey FOREIGN KEY (shard_id) REFERENCES public.stream_shards(shard_id) ON DELETE CASCADE;


--
-- Name: stream_shards stream_shards_table_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_shards
    ADD CONSTRAINT stream_shards_table_id_fkey FOREIGN KEY (table_id) REFERENCES public.tables(table_id) ON DELETE CASCADE;


--
-- Name: tables tables_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tables
    ADD CONSTRAINT tables_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.accounts(account_id) ON DELETE CASCADE;


--
-- Name: vector_indexes vector_indexes_table_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vector_indexes
    ADD CONSTRAINT vector_indexes_table_id_fkey FOREIGN KEY (table_id) REFERENCES public.tables(table_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


