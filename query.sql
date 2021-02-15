
CREATE TABLE public.user_conflict
(
    u_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    display_name character varying(30) COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    email character varying(100) COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    CONSTRAINT user_conflict_pkey PRIMARY KEY (u_id),
    CONSTRAINT user_conflict_displayname_key UNIQUE (display_name),
    CONSTRAINT user_conflict_email_key UNIQUE (email)
)
WITH (
    OIDS = FALSE
)