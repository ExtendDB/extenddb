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
-- Name: _ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773" (
    pk text NOT NULL,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" (
    pk text NOT NULL,
    sk_s text NOT NULL,
    sk_n numeric,
    sk_b bytea,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_b0510e58-91bd-445c-bd2c-da61162138cf; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" (
    pk text NOT NULL,
    sk_s text,
    sk_n numeric,
    sk_b bytea,
    base_pk text NOT NULL,
    base_sk_s text NOT NULL,
    base_sk_n numeric,
    base_sk_b bytea,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" (
    pk text NOT NULL,
    sk_s text,
    sk_n numeric,
    sk_b bytea,
    base_pk text NOT NULL,
    base_sk_s text NOT NULL,
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
-- Data for Name: _ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773" VALUES ('0', '{"v": {"S": "x"}, "id": {"N": "0"}}');
INSERT INTO public."_ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773" VALUES ('1', '{"v": {"S": "x"}, "id": {"N": "1"}}');
INSERT INTO public."_ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773" VALUES ('2', '{"v": {"S": "x"}, "id": {"N": "2"}}');


--
-- Data for Name: _ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" VALUES ('seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_b0510e58-91bd-445c-bd2c-da61162138cf; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p0', NULL, 100, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p1', NULL, 101, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p2', NULL, 102, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p0', NULL, 103, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p1', NULL, 104, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p2', NULL, 105, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p0', NULL, 106, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p1', NULL, 107, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p2', NULL, 108, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" VALUES ('seed-p0', NULL, 109, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g0', 's000', NULL, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g1', 's001', NULL, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g0', 's002', NULL, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g1', 's003', NULL, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g0', 's004', NULL, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g0', 's006', NULL, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g1', 's005', NULL, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g1', 's007', NULL, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g0', 's008', NULL, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" VALUES ('g1', 's009', NULL, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: gsi_pending; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_data_schema.sql', '2026-10-29 07:19:54.790523+00');
INSERT INTO public.schema_history VALUES ('002_gsi_pending.sql', '2026-10-29 07:19:54.810823+00');
INSERT INTO public.schema_history VALUES ('003_idempotency_account_scope.sql', '2026-10-29 07:19:54.830658+00');
INSERT INTO public.schema_history VALUES ('004_vector_index_state.sql', '2026-10-29 07:19:54.856975+00');
INSERT INTO public.schema_history VALUES ('003_gsi_base_key_index', '2026-10-29 07:19:54.863633+00');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394726997', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "100ee800-e9a2-42dd-91f6-7ba0a0ecfc72", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}, "SizeBytes": 46, "SequenceNumber": "000001793258394726997", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.677827+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258394726998', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "320b53cb-46e1-44bf-a24f-c78b9d841571", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394726998", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.684056+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394726999', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "66fd0799-f1f9-4d14-abf9-df1927b97b1c", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394726999", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.689723+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394727000', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "eaa4c326-d9d6-4ff0-b32b-982be0e6091e", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394727000", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.693106+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258394727001', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "8f942e89-0022-49dc-96a4-3a8ebf0df274", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394727001", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.697655+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394727002', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "09be7765-3616-450e-affa-4e1ed6936529", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394727002", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.701162+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394727003', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "5bfe5ce7-c659-4d95-9041-12a258eb2c5d", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394727003", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.70446+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258394727004', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "f1421b89-adb1-43a7-94b0-15928b024e58", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394727004", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.709231+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394727005', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "39a8878e-a194-499f-a779-d570e9beaee2", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394727005", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.712536+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394727006', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', 'Insert', '{"eventID": "1c8324e6-6303-4852-bd17-fef1e692567f", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394727006", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.715953+00');


--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000000', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', NULL, '000000000000000000000', NULL, '2026-10-29 07:19:59.609731+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000001', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', NULL, '000000000000000000000', NULL, '2026-10-29 07:19:59.609731+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000002', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', NULL, '000000000000000000000', NULL, '2026-10-29 07:19:59.609731+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000003', '2ba73e4b-7be3-4c5d-9c56-2b769ab930c6', NULL, '000000000000000000000', NULL, '2026-10-29 07:19:59.609731+00');


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

SELECT pg_catalog.setval('public.stream_seq', 1793258394727006, true);


--
-- Name: _ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773 _ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773"
    ADD CONSTRAINT "_ddb_06587d2c-4cbc-4c97-96e4-d55d7e451773_pkey" PRIMARY KEY (pk);


--
-- Name: _ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6 _ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6"
    ADD CONSTRAINT "_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6_pkey" PRIMARY KEY (pk, sk_s);


--
-- Name: _ddb_b0510e58-91bd-445c-bd2c-da61162138cf _ddb_b0510e58-91bd-445c-bd2c-da61162138cf_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf"
    ADD CONSTRAINT "_ddb_b0510e58-91bd-445c-bd2c-da61162138cf_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0 _ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0"
    ADD CONSTRAINT "_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


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
-- Name: _ddb_b0510e58-91bd-445c-bd2c-da61162138cf_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_b0510e58-91bd-445c-bd2c-da61162138cf_base_key_idx" ON public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_b0510e58-91bd-445c-bd2c-da61_pk_sk_n_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_b0510e58-91bd-445c-bd2c-da61_pk_sk_n_base_pk_base_sk_s_idx" ON public."_ddb_b0510e58-91bd-445c-bd2c-da61162138cf" USING btree (pk, sk_n, base_pk, base_sk_s);


--
-- Name: _ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0_base_key_idx" ON public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_fa93c5ee-d148-407d-bd04-f88e_pk_sk_s_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_fa93c5ee-d148-407d-bd04-f88e_pk_sk_s_base_pk_base_sk_s_idx" ON public."_ddb_fa93c5ee-d148-407d-bd04-f88e417d22d0" USING btree (pk, sk_s, base_pk, base_sk_s);


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
-- Name: idx_ttl__ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "idx_ttl__ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" ON public."_ddb_2ba73e4b-7be3-4c5d-9c56-2b769ab930c6" USING btree (((((item_data -> 'exp'::text) ->> 'N'::text))::bigint)) WHERE (((item_data -> 'exp'::text) ->> 'N'::text) IS NOT NULL);


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


