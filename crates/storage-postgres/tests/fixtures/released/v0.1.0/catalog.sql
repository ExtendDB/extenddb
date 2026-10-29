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
    created_at timestamp with time zone DEFAULT now() NOT NULL
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
    ttl_index_ready boolean DEFAULT false NOT NULL
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
-- Data for Name: access_keys; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: accounts; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.accounts VALUES ('111122223333', 'default', '2026-10-29 07:20:22.642427+00');


--
-- Data for Name: admin_users; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.admin_users VALUES ('admin', '$2b$04$KQg8SAyQ3g2v0nmyz2wPXOWb2pGZtfJ0yLe6E8V1OAQmvQjQlZ9ka', '2026-10-29 07:20:22.643792+00');


--
-- Data for Name: backup_items; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: backups; Type: TABLE DATA; Schema: public; Owner: -
--



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



--
-- Data for Name: idempotency_tokens; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: indexes; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: login_attempts; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: metrics; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: schema_history; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_history VALUES ('001_schema.sql', '2026-10-29 07:20:22.565067+00');


--
-- Data for Name: settings; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.settings VALUES ('catalog_version', '0.0.2');
INSERT INTO public.settings VALUES ('control_plane_delay_seconds', '0.25');
INSERT INTO public.settings VALUES ('gsi_propagation_delay_ms', '10');
INSERT INTO public.settings VALUES ('encryption_key', 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=');
INSERT INTO public.settings VALUES ('data_database_connection_string', 'postgresql://extenddb_app:fixture@fixture-host:5432/extenddb');
INSERT INTO public.settings VALUES ('data_database_name', 'extenddb');


--
-- Data for Name: stream_records; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: stream_shards; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: tables; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: tags; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Name: stream_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.stream_seq', 1793258421032670, true);


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
-- PostgreSQL database dump complete
--


