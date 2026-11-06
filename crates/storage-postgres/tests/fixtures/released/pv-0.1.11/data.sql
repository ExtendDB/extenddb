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
-- Name: _ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7" (
    pk text NOT NULL,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" (
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
-- Name: _ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" (
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
-- Name: _ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" (
    pk text NOT NULL,
    sk_s text NOT NULL,
    sk_n numeric,
    sk_b bytea,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1" (
    pk text NOT NULL,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2" (
    part bytea NOT NULL,
    base_pk text NOT NULL,
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
-- Data for Name: _ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7" VALUES ('0', '{"v": {"S": "x"}, "id": {"N": "0"}}');
INSERT INTO public."_ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7" VALUES ('1', '{"v": {"S": "x"}, "id": {"N": "1"}}');
INSERT INTO public."_ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7" VALUES ('2', '{"v": {"S": "x"}, "id": {"N": "2"}}');


--
-- Data for Name: _ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p0', NULL, 100, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p1', NULL, 101, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p2', NULL, 102, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p0', NULL, 103, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p1', NULL, 104, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p2', NULL, 105, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p0', NULL, 106, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p1', NULL, 107, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p2', NULL, 108, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" VALUES ('seed-p0', NULL, 109, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g0', 's000', NULL, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g1', 's001', NULL, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g0', 's002', NULL, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g1', 's003', NULL, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g0', 's004', NULL, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g1', 's005', NULL, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g0', 's006', NULL, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g1', 's007', NULL, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g0', 's008', NULL, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" VALUES ('g1', 's009', NULL, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" VALUES ('seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1" VALUES ('v0', '{"pk": {"S": "v0"}, "emb": {"L": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}]}}');
INSERT INTO public."_ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1" VALUES ('v1', '{"pk": {"S": "v1"}, "emb": {"L": [{"N": "0.2"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}]}}');
INSERT INTO public."_ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1" VALUES ('v2', '{"pk": {"S": "v2"}, "emb": {"L": [{"N": "0.30000000000000004"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}]}}');


--
-- Data for Name: _ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2" VALUES ('\x00616c6c', 'v0', '[0.1,0.2,0.3,0.4]', 0.547722578048706, '{"pk": {"S": "v0"}}');
INSERT INTO public."_ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2" VALUES ('\x00616c6c', 'v1', '[0.2,0.2,0.3,0.4]', 0.5744562745094299, '{"pk": {"S": "v1"}}');
INSERT INTO public."_ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2" VALUES ('\x00616c6c', 'v2', '[0.3,0.2,0.3,0.4]', 0.6164414286613464, '{"pk": {"S": "v2"}}');


--
-- Data for Name: gsi_pending; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_data_schema.sql', '2026-10-29 07:20:25.387669+00');
INSERT INTO public.schema_history VALUES ('002_gsi_pending.sql', '2026-10-29 07:20:25.402276+00');
INSERT INTO public.schema_history VALUES ('003_idempotency_account_scope.sql', '2026-10-29 07:20:25.41619+00');
INSERT INTO public.schema_history VALUES ('004_vector_index_state.sql', '2026-10-29 07:20:25.429355+00');
INSERT INTO public.schema_history VALUES ('003_gsi_base_key_index', '2026-10-29 07:20:25.435994+00');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258425352460', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "91354c46-92be-4abe-a6b9-efd8e3262902", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}, "SizeBytes": 46, "SequenceNumber": "000001793258425352460", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.096419+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258425352461', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "7a822aa7-8ace-419c-bad5-477a15af379b", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}, "SizeBytes": 47, "SequenceNumber": "000001793258425352461", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.10215+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258425352462', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "200d0e1b-a5f9-442b-b771-b7095a1f1555", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}, "SizeBytes": 47, "SequenceNumber": "000001793258425352462", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.107212+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258425352463', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "e03cd727-c547-4151-8e48-510acd7b0c3d", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}, "SizeBytes": 47, "SequenceNumber": "000001793258425352463", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.11179+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258425352464', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "85b9004f-53f2-4b0b-8c5c-2d36c13b2410", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}, "SizeBytes": 47, "SequenceNumber": "000001793258425352464", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.115193+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258425352465', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "4e4afeee-e2d0-47b9-a304-4633fa0bdbf7", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}, "SizeBytes": 47, "SequenceNumber": "000001793258425352465", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.11965+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258425352466', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "95702c70-c165-4abc-9db9-cc05d6298ca9", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}, "SizeBytes": 47, "SequenceNumber": "000001793258425352466", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.12289+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258425352467', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "dc31d785-8f24-4538-b438-95f19044e958", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}, "SizeBytes": 47, "SequenceNumber": "000001793258425352467", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.12608+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258425352468', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "af032a8e-aa8b-4f00-ba46-285d0eeac323", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}, "SizeBytes": 47, "SequenceNumber": "000001793258425352468", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.130519+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258425352469', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', 'Insert', '{"eventID": "91ecfba7-e1b9-43e2-9c0c-c174bf72af3f", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}, "SizeBytes": 47, "SequenceNumber": "000001793258425352469", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258430}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:30.135253+00');


--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000000', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:29.029253+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000001', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:29.029253+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000002', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:29.029253+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000003', 'd1eba15d-d07e-458c-b75c-8ed7f89f03ec', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:29.029253+00');


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

SELECT pg_catalog.setval('public.stream_seq', 1793258425352469, true);


--
-- Name: _ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7 _ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7"
    ADD CONSTRAINT "_ddb_1fdc2b2f-ad9a-4e71-9634-8346f4dfe3f7_pkey" PRIMARY KEY (pk);


--
-- Name: _ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9 _ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9"
    ADD CONSTRAINT "_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a _ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a"
    ADD CONSTRAINT "_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec _ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec"
    ADD CONSTRAINT "_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec_pkey" PRIMARY KEY (pk, sk_s);


--
-- Name: _ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1 _ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1"
    ADD CONSTRAINT "_ddb_dfd91b2f-8fd4-4426-b581-462393bf43f1_pkey" PRIMARY KEY (pk);


--
-- Name: _ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2 _ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2"
    ADD CONSTRAINT "_ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2_pkey" PRIMARY KEY (base_pk);


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
-- Name: _ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9_base_key_idx" ON public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_6e847a36-5672-4e3d-9ec1-d7b8_pk_sk_n_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_6e847a36-5672-4e3d-9ec1-d7b8_pk_sk_n_base_pk_base_sk_s_idx" ON public."_ddb_6e847a36-5672-4e3d-9ec1-d7b859c0b0c9" USING btree (pk, sk_n, base_pk, base_sk_s);


--
-- Name: _ddb_96b74cd9-0ad7-4b5f-ad43-8f1c_pk_sk_s_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_96b74cd9-0ad7-4b5f-ad43-8f1c_pk_sk_s_base_pk_base_sk_s_idx" ON public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" USING btree (pk, sk_s, base_pk, base_sk_s);


--
-- Name: _ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a_base_key_idx" ON public."_ddb_96b74cd9-0ad7-4b5f-ad43-8f1cbd93ab2a" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2_part_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2_part_idx" ON public."_ddb_vec_3b45415f-d636-4198-b6a6-5c7b515abdb2" USING btree (part);


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
-- Name: idx_ttl__ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "idx_ttl__ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" ON public."_ddb_d1eba15d-d07e-458c-b75c-8ed7f89f03ec" USING btree (((((item_data -> 'exp'::text) ->> 'N'::text))::bigint)) WHERE (((item_data -> 'exp'::text) ->> 'N'::text) IS NOT NULL);


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


