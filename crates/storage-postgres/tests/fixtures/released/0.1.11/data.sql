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
-- Name: _ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" (
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
-- Name: _ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6" (
    pk text NOT NULL,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_c6f40bd3-a950-472d-825d-012dfa26de08; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" (
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
-- Name: _ddb_f98cd4da-fd59-4d78-b38b-074badab0471; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" (
    pk text NOT NULL,
    sk_s text NOT NULL,
    sk_n numeric,
    sk_b bytea,
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
-- Data for Name: _ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g0', 's000', NULL, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g1', 's001', NULL, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g0', 's002', NULL, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g1', 's003', NULL, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g0', 's004', NULL, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g1', 's005', NULL, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g0', 's006', NULL, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g0', 's008', NULL, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g1', 's007', NULL, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" VALUES ('g1', 's009', NULL, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6" VALUES ('0', '{"v": {"S": "x"}, "id": {"N": "0"}}');
INSERT INTO public."_ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6" VALUES ('1', '{"v": {"S": "x"}, "id": {"N": "1"}}');
INSERT INTO public."_ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6" VALUES ('2', '{"v": {"S": "x"}, "id": {"N": "2"}}');


--
-- Data for Name: _ddb_c6f40bd3-a950-472d-825d-012dfa26de08; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p0', NULL, 100, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p1', NULL, 101, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p2', NULL, 102, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p0', NULL, 103, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p1', NULL, 104, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p2', NULL, 105, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p0', NULL, 106, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p1', NULL, 107, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p2', NULL, 108, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" VALUES ('seed-p0', NULL, 109, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_f98cd4da-fd59-4d78-b38b-074badab0471; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" VALUES ('seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: gsi_pending; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_data_schema.sql', '2026-10-29 07:19:53.012472+00');
INSERT INTO public.schema_history VALUES ('002_gsi_pending.sql', '2026-10-29 07:19:53.026985+00');
INSERT INTO public.schema_history VALUES ('003_idempotency_account_scope.sql', '2026-10-29 07:19:53.041932+00');
INSERT INTO public.schema_history VALUES ('004_vector_index_state.sql', '2026-10-29 07:19:53.055008+00');
INSERT INTO public.schema_history VALUES ('003_gsi_base_key_index', '2026-10-29 07:19:53.061913+00');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258392978072', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "4626921b-4fd1-496b-8b57-eabf4221b98c", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}, "SizeBytes": 46, "SequenceNumber": "000001793258392978072", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.239438+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258392978073', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "9e498ba3-1f03-46d0-b02c-5c6a2d227855", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}, "SizeBytes": 47, "SequenceNumber": "000001793258392978073", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.24558+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258392978074', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "0a44d4b0-a8a7-4a1c-a514-5801477f9ac4", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}, "SizeBytes": 47, "SequenceNumber": "000001793258392978074", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.250074+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258392978075', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "915dd686-b5d7-4270-8e71-e1828648fc2f", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}, "SizeBytes": 47, "SequenceNumber": "000001793258392978075", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.255102+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258392978076', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "f323f250-1946-4182-9893-1fb04d409c5e", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}, "SizeBytes": 47, "SequenceNumber": "000001793258392978076", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.259774+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258392978077', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "d9d58e4c-a3db-4942-b0d2-88241a79679f", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}, "SizeBytes": 47, "SequenceNumber": "000001793258392978077", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.263342+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258392978078', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "01b8b921-da98-40df-bc45-630f4d798658", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}, "SizeBytes": 47, "SequenceNumber": "000001793258392978078", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.267194+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258392978079', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "624ffeb8-1798-49d1-b40c-f49e404c9478", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}, "SizeBytes": 47, "SequenceNumber": "000001793258392978079", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.271974+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258392978080', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "f1088f65-a82b-4f5f-b18e-1dde7c7519c0", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}, "SizeBytes": 47, "SequenceNumber": "000001793258392978080", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.276135+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258392978081', 'f98cd4da-fd59-4d78-b38b-074badab0471', 'Insert', '{"eventID": "dfaae2e4-b23d-483d-828f-3ad3b8445655", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}, "SizeBytes": 47, "SequenceNumber": "000001793258392978081", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.280509+00');


--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000000', 'f98cd4da-fd59-4d78-b38b-074badab0471', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:00.164402+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000001', 'f98cd4da-fd59-4d78-b38b-074badab0471', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:00.164402+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000002', 'f98cd4da-fd59-4d78-b38b-074badab0471', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:00.164402+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000003', 'f98cd4da-fd59-4d78-b38b-074badab0471', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:00.164402+00');


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

SELECT pg_catalog.setval('public.stream_seq', 1793258392978081, true);


--
-- Name: _ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95 _ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95"
    ADD CONSTRAINT "_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6 _ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6"
    ADD CONSTRAINT "_ddb_98a10bcc-94ea-46c4-8659-e924ad1b28f6_pkey" PRIMARY KEY (pk);


--
-- Name: _ddb_c6f40bd3-a950-472d-825d-012dfa26de08 _ddb_c6f40bd3-a950-472d-825d-012dfa26de08_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08"
    ADD CONSTRAINT "_ddb_c6f40bd3-a950-472d-825d-012dfa26de08_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_f98cd4da-fd59-4d78-b38b-074badab0471 _ddb_f98cd4da-fd59-4d78-b38b-074badab0471_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471"
    ADD CONSTRAINT "_ddb_f98cd4da-fd59-4d78-b38b-074badab0471_pkey" PRIMARY KEY (pk, sk_s);


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
-- Name: _ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95_base_key_idx" ON public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_02d624d6-cbe9-47b1-b29a-660e_pk_sk_s_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_02d624d6-cbe9-47b1-b29a-660e_pk_sk_s_base_pk_base_sk_s_idx" ON public."_ddb_02d624d6-cbe9-47b1-b29a-660e55b59e95" USING btree (pk, sk_s, base_pk, base_sk_s);


--
-- Name: _ddb_c6f40bd3-a950-472d-825d-012d_pk_sk_n_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_c6f40bd3-a950-472d-825d-012d_pk_sk_n_base_pk_base_sk_s_idx" ON public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" USING btree (pk, sk_n, base_pk, base_sk_s);


--
-- Name: _ddb_c6f40bd3-a950-472d-825d-012dfa26de08_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_c6f40bd3-a950-472d-825d-012dfa26de08_base_key_idx" ON public."_ddb_c6f40bd3-a950-472d-825d-012dfa26de08" USING btree (base_pk, base_sk_s);


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
-- Name: idx_ttl__ddb_f98cd4da-fd59-4d78-b38b-074badab0471; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "idx_ttl__ddb_f98cd4da-fd59-4d78-b38b-074badab0471" ON public."_ddb_f98cd4da-fd59-4d78-b38b-074badab0471" USING btree (((((item_data -> 'exp'::text) ->> 'N'::text))::bigint)) WHERE (((item_data -> 'exp'::text) ->> 'N'::text) IS NOT NULL);


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


