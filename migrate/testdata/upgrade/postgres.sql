--
-- PostgreSQL database dump
--

\restrict XTGyW94xDa2gj6VnMiahpttgAiWCYyNSaJZRIUMbrvPDKOecbcevcr53BAtW8HH

-- Dumped from database version 16.15
-- Dumped by pg_dump version 16.15

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_table_access_method = heap;

--
-- Name: legacy_audit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.legacy_audit (
    id integer NOT NULL,
    note character varying(100) NOT NULL
);


--
-- Name: legacy_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.legacy_users (
    id integer NOT NULL,
    name character varying(100) NOT NULL
);


--
-- Name: migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.migrations (
    id text NOT NULL,
    applied_at timestamp with time zone
);


--
-- Data for Name: legacy_audit; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.legacy_audit VALUES (1, 'created by 3');
INSERT INTO public.legacy_audit VALUES (4, 'long id');


--
-- Data for Name: legacy_users; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.legacy_users VALUES (1, 'alice');
INSERT INTO public.legacy_users VALUES (2, 'Zoë 東京');
INSERT INTO public.legacy_users VALUES (3, 'untouched');


--
-- Data for Name: migrations; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.migrations VALUES ('1_create_users', '2026-11-09 14:26:50.934966+00');
INSERT INTO public.migrations VALUES ('2_seed_users', '2026-11-09 14:26:52.043798+00');
INSERT INTO public.migrations VALUES ('3_create_audit', '2026-11-09 14:26:53.158802+00');
INSERT INTO public.migrations VALUES ('4_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_long_migration_id_', '2026-11-09 14:26:54.267874+00');


--
-- Name: legacy_audit legacy_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.legacy_audit
    ADD CONSTRAINT legacy_audit_pkey PRIMARY KEY (id);


--
-- Name: legacy_users legacy_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.legacy_users
    ADD CONSTRAINT legacy_users_pkey PRIMARY KEY (id);


--
-- Name: migrations migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.migrations
    ADD CONSTRAINT migrations_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict XTGyW94xDa2gj6VnMiahpttgAiWCYyNSaJZRIUMbrvPDKOecbcevcr53BAtW8HH

