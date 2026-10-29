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
-- Name: _ddb_849e9859-89af-4788-9b97-5e9cb9797534; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" (
    pk text NOT NULL,
    sk_s text NOT NULL,
    sk_n numeric,
    sk_b bytea,
    item_data jsonb NOT NULL
);


--
-- Name: _ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" (
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
-- Name: _ddb_aab61207-1400-4488-b9e0-134eb7bb135e; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" (
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
-- Name: _ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7" (
    pk text NOT NULL,
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
-- Data for Name: _ddb_849e9859-89af-4788-9b97-5e9cb9797534; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" VALUES ('seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p0', NULL, 100, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p1', NULL, 101, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p2', NULL, 102, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p0', NULL, 103, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p1', NULL, 104, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p2', NULL, 105, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p0', NULL, 106, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p1', NULL, 107, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p2', NULL, 108, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" VALUES ('seed-p0', NULL, 109, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_aab61207-1400-4488-b9e0-134eb7bb135e; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g0', 's000', NULL, NULL, 'seed-p0', 's000', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}');
INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g1', 's001', NULL, NULL, 'seed-p1', 's001', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}');
INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g0', 's002', NULL, NULL, 'seed-p2', 's002', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}');
INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g1', 's003', NULL, NULL, 'seed-p0', 's003', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}');
INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g1', 's005', NULL, NULL, 'seed-p2', 's005', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}');
INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g0', 's004', NULL, NULL, 'seed-p1', 's004', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}');
INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g0', 's006', NULL, NULL, 'seed-p0', 's006', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}');
INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g1', 's007', NULL, NULL, 'seed-p1', 's007', NULL, NULL, '{"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}');
INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g0', 's008', NULL, NULL, 'seed-p2', 's008', NULL, NULL, '{"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}');
INSERT INTO public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" VALUES ('g1', 's009', NULL, NULL, 'seed-p0', 's009', NULL, NULL, '{"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}');


--
-- Data for Name: _ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public."_ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7" VALUES ('0', '{"v": {"S": "x"}, "id": {"N": "0"}}');
INSERT INTO public."_ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7" VALUES ('1', '{"v": {"S": "x"}, "id": {"N": "1"}}');
INSERT INTO public."_ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7" VALUES ('2', '{"v": {"S": "x"}, "id": {"N": "2"}}');


--
-- Data for Name: gsi_pending; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_data_schema.sql', '2026-10-29 07:19:54.555135+00');
INSERT INTO public.schema_history VALUES ('002_gsi_pending.sql', '2026-10-29 07:19:54.577869+00');
INSERT INTO public.schema_history VALUES ('003_idempotency_account_scope.sql', '2026-10-29 07:19:54.595189+00');
INSERT INTO public.schema_history VALUES ('003_gsi_base_key_index', '2026-10-29 07:19:54.602443+00');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394497902', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "d28f6c12-916c-4965-b2cf-a71877ed8b1b", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s000"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "100"}, "payload": {"S": "item 0"}}, "SizeBytes": 46, "SequenceNumber": "000001793258394497902", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.047251+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258394497903', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "4065f32d-f003-48d4-bdbd-e0f722cdadc4", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s001"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "101"}, "payload": {"S": "item 1"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394497903", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.054041+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394497904', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "860d2e86-e1c1-409a-9551-6141c780f7ed", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s002"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "102"}, "payload": {"S": "item 2"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394497904", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.059282+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394497905', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "4f59d6ac-acb8-4309-8db4-6fc116d18af1", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s003"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "103"}, "payload": {"S": "item 3"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394497905", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.064113+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258394497906', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "7375ed7e-53a9-46f5-94ec-8bcae02dc307", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s004"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "104"}, "payload": {"S": "item 4"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394497906", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.067384+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394497907', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "6fa68ca4-4421-4260-97f1-a38633632e4c", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s005"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "105"}, "payload": {"S": "item 5"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394497907", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.070693+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394497908', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "617398d6-eea6-45a5-bb5c-a478300dfa02", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s006"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "106"}, "payload": {"S": "item 6"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394497908", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.076325+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000003', '000001793258394497909', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "0fc34faf-0d8c-4971-a626-4e9e936a333a", "dynamodb": {"Keys": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}}, "NewImage": {"pk": {"S": "seed-p1"}, "sk": {"S": "s007"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "107"}, "payload": {"S": "item 7"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394497909", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.081311+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394497910', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "6620f534-e40b-4bc5-9eb6-03e19beb67e7", "dynamodb": {"Keys": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}}, "NewImage": {"pk": {"S": "seed-p2"}, "sk": {"S": "s008"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g0"}, "lsk": {"N": "108"}, "payload": {"S": "item 8"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394497910", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.084743+00');
INSERT INTO public.stream_records VALUES ('shardId-seed_tbl-0000000000000001', '000001793258394497911', '849e9859-89af-4788-9b97-5e9cb9797534', 'Insert', '{"eventID": "5c5b8f6d-74c3-4aef-a20a-7bc756695025", "dynamodb": {"Keys": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}}, "NewImage": {"pk": {"S": "seed-p0"}, "sk": {"S": "s009"}, "exp": {"N": "4102444800"}, "gpk": {"S": "g1"}, "lsk": {"N": "109"}, "payload": {"S": "item 9"}}, "SizeBytes": 47, "SequenceNumber": "000001793258394497911", "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": 1793258400}, "awsRegion": "us-east-1", "eventName": "INSERT", "eventSource": "aws:dynamodb", "eventVersion": "1.1"}', '2026-10-29 07:20:00.089374+00');


--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000000', '849e9859-89af-4788-9b97-5e9cb9797534', NULL, '000000000000000000000', NULL, '2026-10-29 07:19:58.977605+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000001', '849e9859-89af-4788-9b97-5e9cb9797534', NULL, '000000000000000000000', NULL, '2026-10-29 07:19:58.977605+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000002', '849e9859-89af-4788-9b97-5e9cb9797534', NULL, '000000000000000000000', NULL, '2026-10-29 07:19:58.977605+00');
INSERT INTO public.stream_shards VALUES ('shardId-seed_tbl-0000000000000003', '849e9859-89af-4788-9b97-5e9cb9797534', NULL, '000000000000000000000', NULL, '2026-10-29 07:19:58.977605+00');


--
-- Name: gsi_pending_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.gsi_pending_id_seq', 10, true);


--
-- Name: stream_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.stream_seq', 1793258394497911, true);


--
-- Name: _ddb_849e9859-89af-4788-9b97-5e9cb9797534 _ddb_849e9859-89af-4788-9b97-5e9cb9797534_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534"
    ADD CONSTRAINT "_ddb_849e9859-89af-4788-9b97-5e9cb9797534_pkey" PRIMARY KEY (pk, sk_s);


--
-- Name: _ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb _ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb"
    ADD CONSTRAINT "_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_aab61207-1400-4488-b9e0-134eb7bb135e _ddb_aab61207-1400-4488-b9e0-134eb7bb135e_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e"
    ADD CONSTRAINT "_ddb_aab61207-1400-4488-b9e0-134eb7bb135e_pkey" PRIMARY KEY (pk, base_pk, base_sk_s);


--
-- Name: _ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7 _ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7"
    ADD CONSTRAINT "_ddb_e4b2cdcf-17a4-4baf-a516-ccd215ec7ca7_pkey" PRIMARY KEY (pk);


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
-- Name: _ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb_base_key_idx" ON public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" USING btree (base_pk, base_sk_s);


--
-- Name: _ddb_a273c554-cea6-4cf7-b8b3-b1a7_pk_sk_n_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_a273c554-cea6-4cf7-b8b3-b1a7_pk_sk_n_base_pk_base_sk_s_idx" ON public."_ddb_a273c554-cea6-4cf7-b8b3-b1a7170ecaeb" USING btree (pk, sk_n, base_pk, base_sk_s);


--
-- Name: _ddb_aab61207-1400-4488-b9e0-134e_pk_sk_s_base_pk_base_sk_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_aab61207-1400-4488-b9e0-134e_pk_sk_s_base_pk_base_sk_s_idx" ON public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" USING btree (pk, sk_s, base_pk, base_sk_s);


--
-- Name: _ddb_aab61207-1400-4488-b9e0-134eb7bb135e_base_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "_ddb_aab61207-1400-4488-b9e0-134eb7bb135e_base_key_idx" ON public."_ddb_aab61207-1400-4488-b9e0-134eb7bb135e" USING btree (base_pk, base_sk_s);


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
-- Name: idx_ttl__ddb_849e9859-89af-4788-9b97-5e9cb9797534; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "idx_ttl__ddb_849e9859-89af-4788-9b97-5e9cb9797534" ON public."_ddb_849e9859-89af-4788-9b97-5e9cb9797534" USING btree (((((item_data -> 'exp'::text) ->> 'N'::text))::bigint)) WHERE (((item_data -> 'exp'::text) ->> 'N'::text) IS NOT NULL);


--
-- Name: stream_records stream_records_shard_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_records
    ADD CONSTRAINT stream_records_shard_id_fkey FOREIGN KEY (shard_id) REFERENCES public.stream_shards(shard_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


