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
-- Name: _ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" (
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
-- Name: _ddb_7ce6911f-6383-43b2-8342-bf82c9792631; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_7ce6911f-6383-43b2-8342-bf82c9792631" (
    pk text NOT NULL,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" (
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
-- Name: _ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" (
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
-- Name: gsi_pending id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gsi_pending ALTER COLUMN id SET DEFAULT nextval('public.gsi_pending_id_seq'::regclass);


--
-- Data for Name: _ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p0', NULL, 100, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p1', NULL, 101, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p2', NULL, 102, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p0', NULL, 103, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p1', NULL, 104, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p2', NULL, 105, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p0', NULL, 106, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p1', NULL, 107, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p2', NULL, 108, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" VALUES ('seed-p0', NULL, 109, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_7ce6911f-6383-43b2-8342-bf82c9792631; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_7ce6911f-6383-43b2-8342-bf82c9792631" VALUES ('0', '{"v": {"S": "x"}, "id": {"N": "0"}}');
INSERT INTO public."_ddb_7ce6911f-6383-43b2-8342-bf82c9792631" VALUES ('1', '{"v": {"S": "x"}, "id": {"N": "1"}}');
INSERT INTO public."_ddb_7ce6911f-6383-43b2-8342-bf82c9792631" VALUES ('2', '{"v": {"S": "x"}, "id": {"N": "2"}}');


--
-- Data for Name: _ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g0', 's000', NULL, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g1', 's001', NULL, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g0', 's002', NULL, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g1', 's003', NULL, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g0', 's004', NULL, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g1', 's005', NULL, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g0', 's006', NULL, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g1', 's007', NULL, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g0', 's008', NULL, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" VALUES ('g1', 's009', NULL, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" VALUES ('seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: gsi_pending; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_data_schema.sql', '2026-10-29 07:19:54.720468+00');
INSERT INTO public.schema_history VALUES ('002_gsi_pending.sql', '2026-10-29 07:19:54.738729+00');
INSERT INTO public.schema_history VALUES ('003_idempotency_account_scope.sql', '2026-10-29 07:19:54.762508+00');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394673916', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "00ffe0b6-dc94-4884-852b-3ecbdfd7ab75", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}, "SizeBytes": 46, "SequenceNumber": "000001793258394673916", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.355898+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258394673917', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "ddb45bf1-061a-4bc5-a725-988303b8bd9d", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394673917", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.362149+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394673918', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "c2359140-6ac5-4c77-946b-5ce89af42b7b", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394673918", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.367886+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394673919', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "bc5ab966-1e62-4d7b-baff-04409d342d21", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394673919", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.372992+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258394673920', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "49029f3a-6ed2-48c5-a3f8-6541815f7e90", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394673920", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.377492+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394673921', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "a12cdf01-964e-4064-b268-524165ee401b", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394673921", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.382156+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394673922', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "6b656af9-9c85-4a9a-9ee5-31d520a0971e", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394673922", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.385416+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258394673923', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "6b9aa409-5e42-4850-95eb-08b3ab3cf5ea", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394673923", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.389117+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394673924', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "74781556-d775-4a14-9aba-32e3612b544f", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394673924", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.393972+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394673925', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', 'Insert', '{"eventID": "b48eeb57-eb91-42d3-b208-e0fd3ac18889", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394673925", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258401}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:01.397207+00');


--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000000', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:00.291571+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000001', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:00.291571+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000002', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:00.291571+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000003', 'f47329bd-63a3-4c7a-a028-dca84bcf1b87', NULL, '000000000000000000000', NULL, '2026-10-29 07:20:00.291571+00');


--
-- Name: gsi_pending_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.gsi_pending_id_seq', 10, true);


--
-- Name: stream_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.stream_seq', 1793258394673925, true);


--
-- Name: _ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a _ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a"
    ADD CONSTRAINT "_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_7ce6911f-6383-43b2-8342-bf82c9792631 _ddb_7ce6911f-6383-43b2-8342-bf82c9792631_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_7ce6911f-6383-43b2-8342-bf82c9792631"
    ADD CONSTRAINT "_ddb_7ce6911f-6383-43b2-8342-bf82c9792631_pkey" PRIMARY KEY (pk);


--
-- Name: _ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e _ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e"
    ADD CONSTRAINT "_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87 _ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87"
    ADD CONSTRAINT "_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87_pkey" PRIMARY KEY (pk, sk_s);


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
-- Name: _ddb_154c901d-438d-4df4-bef3-a09e_pk_sk_n_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_154c901d-438d-4df4-bef3-a09e_pk_sk_n_base_pk_base_sk_s_idx" ON public."_ddb_154c901d-438d-4df4-bef3-a09eccbe5e4a" USING btree (pk, sk_n, base_pk, base_sk_s);


--
-- Name: _ddb_c440580e-afe8-49b4-bc9d-1b5b_pk_sk_s_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_c440580e-afe8-49b4-bc9d-1b5b_pk_sk_s_base_pk_base_sk_s_idx" ON public."_ddb_c440580e-afe8-49b4-bc9d-1b5bfecb229e" USING btree (pk, sk_s, base_pk, base_sk_s);


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
-- Name: idx_ttl__ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "idx_ttl__ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" ON public."_ddb_f47329bd-63a3-4c7a-a028-dca84bcf1b87" USING btree (((((item_data -> 'exp'::text) ->> 'N'::text))::bigint)) WHERE (((item_data -> 'exp'::text) ->> 'N'::text) IS NOT NULL);


--
-- Name: stream_records stream_records_shard_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_records
    ADD CONSTRAINT stream_records_shard_id_fkey FOREIGN KEY (shard_id) REFERENCES public.stream_shards(shard_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


