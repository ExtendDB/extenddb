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
-- Name: _ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" (
    pk text NOT NULL COLLATE pg_catalog."C",
    sk_s text NOT NULL COLLATE pg_catalog."C",
    sk_n numeric,
    sk_b bytea,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf" (
    pk text NOT NULL COLLATE pg_catalog."C",
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" (
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
-- Name: _ddb_dc13e7ee-6af3-46af-aca3-1227b2965484; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" (
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
-- Data for Name: _ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" VALUES ('seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf" VALUES ('0', '{"v": {"S": "x"}, "id": {"N": "0"}}');
INSERT INTO public."_ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf" VALUES ('1', '{"v": {"S": "x"}, "id": {"N": "1"}}');
INSERT INTO public."_ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf" VALUES ('2', '{"v": {"S": "x"}, "id": {"N": "2"}}');


--
-- Data for Name: _ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p0', NULL, 100, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p1', NULL, 101, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p2', NULL, 102, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p0', NULL, 103, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p1', NULL, 104, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p2', NULL, 105, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p0', NULL, 106, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p1', NULL, 107, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p2', NULL, 108, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" VALUES ('seed-p0', NULL, 109, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_dc13e7ee-6af3-46af-aca3-1227b2965484; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g0', 's000', NULL, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g1', 's001', NULL, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g0', 's002', NULL, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g1', 's003', NULL, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g0', 's004', NULL, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g1', 's005', NULL, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g0', 's006', NULL, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g1', 's007', NULL, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g0', 's008', NULL, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" VALUES ('g1', 's009', NULL, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: gsi_pending; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_data_schema.sql', '2026-10-29 07:20:20.4192+00');
INSERT INTO public.schema_history VALUES ('002_gsi_pending.sql', '2026-10-29 07:20:20.45114+00');
INSERT INTO public.schema_history VALUES ('003_idempotency_account_scope.sql', '2026-10-29 07:20:20.482139+00');
INSERT INTO public.schema_history VALUES ('004_vector_index_state.sql', '2026-10-29 07:20:20.504433+00');
INSERT INTO public.schema_history VALUES ('003_gsi_base_key_index', '2026-10-29 07:20:20.511687+00');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000001', '000001793258420353575', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "c7cc3bd8-265c-4b6a-9ca4-9de56a0122ef", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}, "SizeBytes": 46, "SequenceNumber": "000001793258420353575", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.67263+00');
INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000003', '000001793258420353576', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "090547fe-a61d-43c1-ac23-999315e98aa4", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}, "SizeBytes": 47, "SequenceNumber": "000001793258420353576", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.679727+00');
INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000001', '000001793258420353577', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "bf2dfef1-a322-4a2c-8129-2e5c6a3314ef", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}, "SizeBytes": 47, "SequenceNumber": "000001793258420353577", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.685198+00');
INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000001', '000001793258420353578', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "685d51f7-6272-4263-a8cd-e8b4f6ab4342", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}, "SizeBytes": 47, "SequenceNumber": "000001793258420353578", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.689415+00');
INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000003', '000001793258420353579', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "f9b272b0-3d9f-41c1-8cac-62ebd5ef9a08", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}, "SizeBytes": 47, "SequenceNumber": "000001793258420353579", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.694621+00');
INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000001', '000001793258420353580', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "0e984e4a-6935-4220-ae0e-44fb7645943f", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}, "SizeBytes": 47, "SequenceNumber": "000001793258420353580", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.698049+00');
INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000001', '000001793258420353581', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "45e8db28-9b2f-4dca-bf2f-50d2f67f50f2", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}, "SizeBytes": 47, "SequenceNumber": "000001793258420353581", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.701315+00');
INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000003', '000001793258420353582', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "ee069e29-86b4-45ae-8550-54cbda8b4618", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}, "SizeBytes": 47, "SequenceNumber": "000001793258420353582", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.704766+00');
INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000001', '000001793258420353583', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "a878a881-284b-480f-b390-894d4ce1e796", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}, "SizeBytes": 47, "SequenceNumber": "000001793258420353583", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.708638+00');
INSERT INTO public.stream_records VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000001', '000001793258420353584', '1b515984-9e84-4b37-a63e-f4a64c5166c2', 'Insert', '{"eventID": "7dfd0256-0a7a-43f7-847a-b61c22f6e317", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}, "SizeBytes": 47, "SequenceNumber": "000001793258420353584", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258427}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:27.712256+00');


--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_shards VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000000', '1b515984-9e84-4b37-a63e-f4a64c5166c2', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:26.60781+00');
INSERT INTO public.stream_shards VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000001', '1b515984-9e84-4b37-a63e-f4a64c5166c2', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:26.60781+00');
INSERT INTO public.stream_shards VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000002', '1b515984-9e84-4b37-a63e-f4a64c5166c2', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:26.60781+00');
INSERT INTO public.stream_shards VALUES ('shardId-1b515984-9e84-4b37-a63e-f4a64c5166c2-0000000000000003', '1b515984-9e84-4b37-a63e-f4a64c5166c2', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:26.60781+00');


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

SELECT pg_catalog.setval('public.stream_seq', 1793258420353584, true);


--
-- Name: _ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2 _ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2"
    ADD CONSTRAINT "_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2_pkey" PRIMARY KEY (pk, sk_s);


--
-- Name: _ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf _ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf"
    ADD CONSTRAINT "_ddb_3fe9fd1a-5831-44e7-b725-0b05c87bddcf_pkey" PRIMARY KEY (pk);


--
-- Name: _ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f _ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f"
    ADD CONSTRAINT "_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_dc13e7ee-6af3-46af-aca3-1227b2965484 _ddb_dc13e7ee-6af3-46af-aca3-1227b2965484_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484"
    ADD CONSTRAINT "_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


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
-- Name: _ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f_base_key_idx" ON public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_d5e71af3-9bec-4a69-8e8b-4b66_pk_sk_n_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_d5e71af3-9bec-4a69-8e8b-4b66_pk_sk_n_base_pk_base_sk_s_idx" ON public."_ddb_d5e71af3-9bec-4a69-8e8b-4b668a393f5f" USING btree (pk, sk_n, base_pk, base_sk_s);


--
-- Name: _ddb_dc13e7ee-6af3-46af-aca3-1227_pk_sk_s_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_dc13e7ee-6af3-46af-aca3-1227_pk_sk_s_base_pk_base_sk_s_idx" ON public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" USING btree (pk, sk_s, base_pk, base_sk_s);


--
-- Name: _ddb_dc13e7ee-6af3-46af-aca3-1227b2965484_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484_base_key_idx" ON public."_ddb_dc13e7ee-6af3-46af-aca3-1227b2965484" USING btree (base_pk, base_sk_s);


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
-- Name: idx_ttl__ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "idx_ttl__ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" ON public."_ddb_1b515984-9e84-4b37-a63e-f4a64c5166c2" USING btree (((((item_data -> 'exp'::text) ->> 'N'::text))::bigint)) WHERE (((item_data -> 'exp'::text) ->> 'N'::text) IS NOT NULL);


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


