CREATE TABLE public.product (
    id integer NOT NULL,
    "created" timestamp without time zone NOT NULL,
    "modified" timestamp without time zone,
    name character varying(100) NOT NULL,
    category character varying(100) NOT NULL,
    price double precision
);

CREATE SEQUENCE public.product_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE ONLY public.product ALTER COLUMN id SET DEFAULT nextval('public.product_id_seq'::regclass);
