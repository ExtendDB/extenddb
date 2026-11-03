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
-- Name: _ddb_1a29dd38-2885-400f-865d-088a6d1337cf; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" (
    pk text NOT NULL COLLATE pg_catalog."C",
    sk_s text NOT NULL COLLATE pg_catalog."C",
    sk_n numeric,
    sk_b bytea,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_56c0164f-e84d-4b14-a0fb-326da3680274; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_56c0164f-e84d-4b14-a0fb-326da3680274" (
    pk text NOT NULL COLLATE pg_catalog."C",
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_6f9f0986-a749-489c-951f-4480ba7fdac2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" (
    pk text NOT NULL COLLATE pg_catalog."C",
    sk_s text COLLATE pg_catalog."C",
    sk_n numeric,
    sk_b bytea,
    base_pk text NOT NULL COLLATE pg_catalog."C",
    base_sk_s text NOT NULL COLLATE pg_catalog."C",
    base_sk_n numeric,
    base_sk_b bytea,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" (
    pk text NOT NULL COLLATE pg_catalog."C",
    sk_s text COLLATE pg_catalog."C",
    sk_n numeric,
    sk_b bytea,
    base_pk text NOT NULL COLLATE pg_catalog."C",
    base_sk_s text NOT NULL COLLATE pg_catalog."C",
    base_sk_n numeric,
    base_sk_b bytea,
    item_data jsonb NOT NULL
);


--
-- Name: gsi_pending; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gsi_pending (
    id bigint NOT NULL,
    table_id text NOT NULL,
    worker_partition integer NOT NULL,
    old_item jsonb,
    new_item jsonb,
    index_context jsonb NOT NULL,
    ready_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: gsi_pending_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.gsi_pending_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gsi_pending_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.gsi_pending_id_seq OWNED BY public.gsi_pending.id;


--
-- Name: idempotency_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.idempotency_tokens (
    account_id text NOT NULL,
    token text NOT NULL,
    fingerprint text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: schema_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_history (
    filename text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
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
-- Name: vector_index_holds; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vector_index_holds (
    table_id text NOT NULL,
    index_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: gsi_pending id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gsi_pending ALTER COLUMN id SET DEFAULT nextval('public.gsi_pending_id_seq'::regclass);


--
-- Data for Name: _ddb_1a29dd38-2885-400f-865d-088a6d1337cf; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" VALUES ('seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_56c0164f-e84d-4b14-a0fb-326da3680274; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_56c0164f-e84d-4b14-a0fb-326da3680274" VALUES ('0', '{"v": {"S": "x"}, "id": {"N": "0"}}');
INSERT INTO public."_ddb_56c0164f-e84d-4b14-a0fb-326da3680274" VALUES ('1', '{"v": {"S": "x"}, "id": {"N": "1"}}');
INSERT INTO public."_ddb_56c0164f-e84d-4b14-a0fb-326da3680274" VALUES ('2', '{"v": {"S": "x"}, "id": {"N": "2"}}');


--
-- Data for Name: _ddb_6f9f0986-a749-489c-951f-4480ba7fdac2; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g0', 's000', NULL, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g1', 's001', NULL, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g0', 's002', NULL, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g1', 's003', NULL, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g0', 's004', NULL, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g1', 's005', NULL, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g0', 's006', NULL, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g0', 's008', NULL, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g1', 's007', NULL, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" VALUES ('g1', 's009', NULL, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p0', NULL, 100, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p1', NULL, 101, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p2', NULL, 102, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p0', NULL, 103, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p1', NULL, 104, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p2', NULL, 105, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p0', NULL, 106, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p1', NULL, 107, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p2', NULL, 108, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" VALUES ('seed-p0', NULL, 109, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: gsi_pending; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_data_schema.sql', '2026-10-29 07:20:24.795649+00');
INSERT INTO public.schema_history VALUES ('002_gsi_pending.sql', '2026-10-29 07:20:24.819409+00');
INSERT INTO public.schema_history VALUES ('003_idempotency_account_scope.sql', '2026-10-29 07:20:24.839746+00');
INSERT INTO public.schema_history VALUES ('004_vector_index_state.sql', '2026-10-29 07:20:24.859668+00');
INSERT INTO public.schema_history VALUES ('003_gsi_base_key_index', '2026-10-29 07:20:24.867762+00');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258424743531', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "64bbd65e-d5ce-49d4-aa0f-5cce40c2b802", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}, "SizeBytes": 46, "SequenceNumber": "000001793258424743531", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.369576+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258424743532', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "3828fc5f-2830-45ab-8f04-acf7cac45fbc", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}, "SizeBytes": 47, "SequenceNumber": "000001793258424743532", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.375214+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258424743533', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "55fad775-23d6-4ca8-bbe3-23c08baf9631", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}, "SizeBytes": 47, "SequenceNumber": "000001793258424743533", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.379776+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258424743534', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "e04dd3d6-e7a6-4bd8-ab15-44626f9e4ab5", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}, "SizeBytes": 47, "SequenceNumber": "000001793258424743534", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.383132+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258424743535', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "56750f44-f671-4a7d-95d1-101e41c909fb", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}, "SizeBytes": 47, "SequenceNumber": "000001793258424743535", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.386476+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258424743536', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "426619bd-a66c-4272-a795-25c62aadc8de", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}, "SizeBytes": 47, "SequenceNumber": "000001793258424743536", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.390656+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258424743537', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "22c5a458-0d6b-44b2-9917-1521e8debba3", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}, "SizeBytes": 47, "SequenceNumber": "000001793258424743537", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.395823+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258424743538', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "25637627-26df-422f-ba7b-624207e44055", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}, "SizeBytes": 47, "SequenceNumber": "000001793258424743538", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.401449+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258424743539', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "1bdc30a7-0436-4281-b064-d36632ff1aad", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}, "SizeBytes": 47, "SequenceNumber": "000001793258424743539", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.404777+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258424743540', '1a29dd38-2885-400f-865d-088a6d1337cf', 'Insert', '{"eventID": "c2ee672f-8a1c-414c-bbb0-903fb7cf3116", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}, "SizeBytes": 47, "SequenceNumber": "000001793258424743540", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.408031+00');


--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000000', '1a29dd38-2885-400f-865d-088a6d1337cf', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:29.303969+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000001', '1a29dd38-2885-400f-865d-088a6d1337cf', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:29.303969+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000002', '1a29dd38-2885-400f-865d-088a6d1337cf', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:29.303969+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000003', '1a29dd38-2885-400f-865d-088a6d1337cf', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:29.303969+00');


--
-- Data for Name: vector_index_holds; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Name: gsi_pending_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.gsi_pending_id_seq', 10, true);


--
-- Name: stream_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.stream_seq', 1793258424743540, true);


--
-- Name: _ddb_1a29dd38-2885-400f-865d-088a6d1337cf _ddb_1a29dd38-2885-400f-865d-088a6d1337cf_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf"
    ADD CONSTRAINT "_ddb_1a29dd38-2885-400f-865d-088a6d1337cf_pkey" PRIMARY KEY (pk, sk_s);


--
-- Name: _ddb_56c0164f-e84d-4b14-a0fb-326da3680274 _ddb_56c0164f-e84d-4b14-a0fb-326da3680274_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_56c0164f-e84d-4b14-a0fb-326da3680274"
    ADD CONSTRAINT "_ddb_56c0164f-e84d-4b14-a0fb-326da3680274_pkey" PRIMARY KEY (pk);


--
-- Name: _ddb_6f9f0986-a749-489c-951f-4480ba7fdac2 _ddb_6f9f0986-a749-489c-951f-4480ba7fdac2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2"
    ADD CONSTRAINT "_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c _ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c"
    ADD CONSTRAINT "_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: gsi_pending gsi_pending_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gsi_pending
    ADD CONSTRAINT gsi_pending_pkey PRIMARY KEY (id);


--
-- Name: idempotency_tokens idempotency_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.idempotency_tokens
    ADD CONSTRAINT idempotency_tokens_pkey PRIMARY KEY (account_id, token);


--
-- Name: schema_history schema_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_history
    ADD CONSTRAINT schema_history_pkey PRIMARY KEY (filename);


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
-- Name: vector_index_holds vector_index_holds_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vector_index_holds
    ADD CONSTRAINT vector_index_holds_pkey PRIMARY KEY (table_id, index_id);


--
-- Name: _ddb_6f9f0986-a749-489c-951f-4480_pk_sk_s_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_6f9f0986-a749-489c-951f-4480_pk_sk_s_base_pk_base_sk_s_idx" ON public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" USING btree (pk, sk_s, base_pk, base_sk_s);


--
-- Name: _ddb_6f9f0986-a749-489c-951f-4480ba7fdac2_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2_base_key_idx" ON public."_ddb_6f9f0986-a749-489c-951f-4480ba7fdac2" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c_base_key_idx" ON public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_ee4a0c37-7c89-416e-aa97-4a01_pk_sk_n_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_ee4a0c37-7c89-416e-aa97-4a01_pk_sk_n_base_pk_base_sk_s_idx" ON public."_ddb_ee4a0c37-7c89-416e-aa97-4a0113f8bd1c" USING btree (pk, sk_n, base_pk, base_sk_s);


--
-- Name: idx_gsi_pending_claim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_gsi_pending_claim ON public.gsi_pending USING btree (worker_partition, ready_at, id);


--
-- Name: idx_idempotency_tokens_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_idempotency_tokens_created ON public.idempotency_tokens USING btree (created_at);


--
-- Name: idx_stream_records_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_records_created ON public.stream_records USING btree (created_at);


--
-- Name: idx_stream_shards_table; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_shards_table ON public.stream_shards USING btree (table_id);


--
-- Name: idx_ttl__ddb_1a29dd38-2885-400f-865d-088a6d1337cf; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "idx_ttl__ddb_1a29dd38-2885-400f-865d-088a6d1337cf" ON public."_ddb_1a29dd38-2885-400f-865d-088a6d1337cf" USING btree (((((item_data -> 'exp'::text) ->> 'N'::text))::bigint)) WHERE (((item_data -> 'exp'::text) ->> 'N'::text) IS NOT NULL);


--
-- Name: idx_vector_index_holds_table; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vector_index_holds_table ON public.vector_index_holds USING btree (table_id);


--
-- Name: stream_records stream_records_shard_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_records
    ADD CONSTRAINT stream_records_shard_id_fkey FOREIGN KEY (shard_id) REFERENCES public.stream_shards(shard_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


