--
-- PostgreSQL database dump
--


-- Dumped from database version 16.15 (Debian 16.15-1.pgdg12+2)
-- Dumped by pg_dump version 16.15 (Debian 16.15-1.pgdg12+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: vector; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;


--
-- Name: EXTENSION vector; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION vector IS 'vector data type and ivfflat and hnsw access methods';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" (
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
-- Name: _ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a" (
    pk text NOT NULL COLLATE pg_catalog."C",
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382" (
    pk text NOT NULL COLLATE pg_catalog."C",
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" (
    pk text NOT NULL COLLATE pg_catalog."C",
    sk_s text NOT NULL COLLATE pg_catalog."C",
    sk_n numeric,
    sk_b bytea,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" (
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
-- Name: _ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e" (
    part bytea NOT NULL,
    base_pk text NOT NULL COLLATE pg_catalog."C",
    embedding public.vector(4) NOT NULL,
    nrm double precision NOT NULL,
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
-- Data for Name: _ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p0', NULL, 100, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p1', NULL, 101, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p2', NULL, 102, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p0', NULL, 103, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p1', NULL, 104, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p2', NULL, 105, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p0', NULL, 106, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p1', NULL, 107, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p2', NULL, 108, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" VALUES ('seed-p0', NULL, 109, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a" VALUES ('0', '{"v": {"S": "x"}, "id": {"N": "0"}}');
INSERT INTO public."_ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a" VALUES ('1', '{"v": {"S": "x"}, "id": {"N": "1"}}');
INSERT INTO public."_ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a" VALUES ('2', '{"v": {"S": "x"}, "id": {"N": "2"}}');


--
-- Data for Name: _ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382" VALUES ('v0', '{"pk": {"S": "v0"}, "emb": {"L": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}]}}');
INSERT INTO public."_ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382" VALUES ('v1', '{"pk": {"S": "v1"}, "emb": {"L": [{"N": "0.2"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}]}}');
INSERT INTO public."_ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382" VALUES ('v2', '{"pk": {"S": "v2"}, "emb": {"L": [{"N": "0.30000000000000004"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}]}}');


--
-- Data for Name: _ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" VALUES ('seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g0', 's000', NULL, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g1', 's001', NULL, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g0', 's002', NULL, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g1', 's003', NULL, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g0', 's004', NULL, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g1', 's005', NULL, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g0', 's006', NULL, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g1', 's007', NULL, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g0', 's008', NULL, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" VALUES ('g1', 's009', NULL, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e" VALUES ('\x00616c6c', 'v0', '[0.1,0.2,0.3,0.4]', 0.547722578048706, '{"pk": {"S": "v0"}}');
INSERT INTO public."_ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e" VALUES ('\x00616c6c', 'v1', '[0.2,0.2,0.3,0.4]', 0.5744562745094299, '{"pk": {"S": "v1"}}');
INSERT INTO public."_ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e" VALUES ('\x00616c6c', 'v2', '[0.3,0.2,0.3,0.4]', 0.6164414286613464, '{"pk": {"S": "v2"}}');


--
-- Data for Name: gsi_pending; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_data_schema.sql', '2026-10-29 07:20:23.837247+00');
INSERT INTO public.schema_history VALUES ('002_gsi_pending.sql', '2026-10-29 07:20:23.851674+00');
INSERT INTO public.schema_history VALUES ('003_idempotency_account_scope.sql', '2026-10-29 07:20:23.865411+00');
INSERT INTO public.schema_history VALUES ('004_vector_index_state.sql', '2026-10-29 07:20:23.878657+00');
INSERT INTO public.schema_history VALUES ('003_gsi_base_key_index', '2026-10-29 07:20:23.885859+00');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000001', '000001793258423801341', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "819c76f0-b73c-4f83-95ef-62554381f2f5", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}, "SizeBytes": 46, "SequenceNumber": "000001793258423801341", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.719082+00');
INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000003', '000001793258423801342', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "4e68c958-0731-4c25-90f7-719df56afa29", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}, "SizeBytes": 47, "SequenceNumber": "000001793258423801342", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.72468+00');
INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000001', '000001793258423801343', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "03e391a9-0720-405b-bbe0-49d8a414ccff", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}, "SizeBytes": 47, "SequenceNumber": "000001793258423801343", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.731027+00');
INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000001', '000001793258423801344', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "545607aa-27db-452b-a40b-4685cfc04741", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}, "SizeBytes": 47, "SequenceNumber": "000001793258423801344", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.734604+00');
INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000003', '000001793258423801345', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "4044929f-0f68-47c4-bf6f-c651ab1e6f54", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}, "SizeBytes": 47, "SequenceNumber": "000001793258423801345", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.741369+00');
INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000001', '000001793258423801346', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "fb4c475d-85e2-414c-8f56-8c3bbab36476", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}, "SizeBytes": 47, "SequenceNumber": "000001793258423801346", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.745888+00');
INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000001', '000001793258423801347', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "e4a77fd8-0099-4c94-be08-682ba2f912d2", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}, "SizeBytes": 47, "SequenceNumber": "000001793258423801347", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.749235+00');
INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000003', '000001793258423801348', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "abd94ae3-1866-4f72-b980-d0f9f592440e", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}, "SizeBytes": 47, "SequenceNumber": "000001793258423801348", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.753673+00');
INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000001', '000001793258423801349', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "31d0cb69-b9a5-42cb-ad2f-afc4db7763a3", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}, "SizeBytes": 47, "SequenceNumber": "000001793258423801349", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.757548+00');
INSERT INTO public.stream_records VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000001', '000001793258423801350', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', 'Insert', '{"eventID": "d95f5ace-26c2-428e-a602-a3cb433de2f1", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}, "SizeBytes": 47, "SequenceNumber": "000001793258423801350", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258429}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:29.760587+00');


--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_shards VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000000', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:28.659262+00');
INSERT INTO public.stream_shards VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000001', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:28.659262+00');
INSERT INTO public.stream_shards VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000002', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:28.659262+00');
INSERT INTO public.stream_shards VALUES ('shardId-e5614f75-20b4-42ec-8cee-38c5a47607dc-0000000000000003', 'e5614f75-20b4-42ec-8cee-38c5a47607dc', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:28.659262+00');


--
-- Data for Name: vector_index_holds; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Name: gsi_pending_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.gsi_pending_id_seq', 13, true);


--
-- Name: stream_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.stream_seq', 1793258423801350, true);


--
-- Name: _ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a _ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a"
    ADD CONSTRAINT "_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a _ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a"
    ADD CONSTRAINT "_ddb_3c6c5804-7d8c-4350-b813-a19e74032d8a_pkey" PRIMARY KEY (pk);


--
-- Name: _ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382 _ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382"
    ADD CONSTRAINT "_ddb_402b7bc8-1c0d-4ea8-90d0-1969c91ea382_pkey" PRIMARY KEY (pk);


--
-- Name: _ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc _ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc"
    ADD CONSTRAINT "_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc_pkey" PRIMARY KEY (pk, sk_s);


--
-- Name: _ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a _ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a"
    ADD CONSTRAINT "_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e _ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e"
    ADD CONSTRAINT "_ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e_pkey" PRIMARY KEY (base_pk);


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
-- Name: _ddb_265ecb86-6caa-4ed0-8cb8-2657_pk_sk_n_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_265ecb86-6caa-4ed0-8cb8-2657_pk_sk_n_base_pk_base_sk_s_idx" ON public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" USING btree (pk, sk_n, base_pk, base_sk_s);


--
-- Name: _ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a_base_key_idx" ON public."_ddb_265ecb86-6caa-4ed0-8cb8-2657d21adf1a" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a_base_key_idx" ON public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_fc7c55b3-dc1f-444c-bbeb-8a78_pk_sk_s_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_fc7c55b3-dc1f-444c-bbeb-8a78_pk_sk_s_base_pk_base_sk_s_idx" ON public."_ddb_fc7c55b3-dc1f-444c-bbeb-8a781208152a" USING btree (pk, sk_s, base_pk, base_sk_s);


--
-- Name: _ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e_part_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e_part_idx" ON public."_ddb_vec_f534d64e-bb45-452c-9578-d21b4ea5fb6e" USING btree (part);


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
-- Name: idx_ttl__ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "idx_ttl__ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" ON public."_ddb_e5614f75-20b4-42ec-8cee-38c5a47607dc" USING btree (((((item_data -> 'exp'::text) ->> 'N'::text))::bigint)) WHERE (((item_data -> 'exp'::text) ->> 'N'::text) IS NOT NULL);


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


