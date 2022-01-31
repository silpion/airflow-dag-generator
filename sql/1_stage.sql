\c stage;

CREATE TABLE public.customer (
    id serial PRIMARY KEY,
    nk_number bigint NOT NULL,
    first_name VARCHAR (255) NOT NULL,
    last_name VARCHAR (255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);

CREATE UNIQUE INDEX idx_customer_nk_number ON public.customer (nk_number);

CREATE TABLE public.article (
    id serial PRIMARY KEY,
    nk_number bigint NOT NULL,
    name VARCHAR (255) NOT NULL,
    description VARCHAR (255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);

CREATE UNIQUE INDEX idx_article_nk_number ON public.article (nk_number);

CREATE TABLE public.store (
    id serial PRIMARY KEY,
    nk_number bigint not null,
    name VARCHAR (255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);

CREATE UNIQUE INDEX idx_store_nk_number ON public.store (nk_number);

CREATE TABLE public.sale (
    id serial PRIMARY KEY,
    nk bigint NOT NULL,
    id_customer bigint NOT NULL,
    id_store bigint NOT NULL,
    id_article bigint NOT NULL,
    sale_date TIMESTAMP WITH TIME ZONE,
    price BIGINT NOT NULL,
    amount BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);

CREATE UNIQUE INDEX idx_sale_nk ON public.sale(nk);

-- Add test data

INSERT INTO public.customer (nk_number, first_name, last_name, created_by)
values
(1234334, 'Kalle', 'Schablonski', 'chris'),
(2121333, 'Bert', 'Bradulska', 'chris'),
(2434455, 'Horst', 'Budsano', 'chris'),
(3435555, 'Dicky', 'Droemel', 'chris'),
(8856565, 'Peter', 'Penunsula', 'chris')
ON CONFLICT (nk_number)
    DO
UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, created_by = EXCLUDED.created_by
;

-----------------------------------------------

INSERT INTO public.article (nk_number, name, description, created_by)
values
(98383, 'Screw', 'Small nice screw', 'chris'),
(121234, 'Nail', 'Bent nail', 'chris'),
(92833, 'Saw', 'Rusty saw', 'chris')
ON CONFLICT (nk_number)
    DO
UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, created_by = EXCLUDED.created_by
;
-------------------------------------------------

INSERT INTO public.store (nk_number, name, created_by)
VALUES ('311', 'Great store', 'chris'),
       ('511', 'Not so great store', 'chris')
ON CONFLICT (nk_number)
DO
   UPDATE SET name = EXCLUDED.name, created_by = EXCLUDED.created_by
;

--------------------------------------------------

INSERT
    INTO public.sale
        (nk, id_customer, id_store, id_article, price, amount, sale_date, created_by)
SELECT
    '12334',
    c.id as id_customer,
    s.id as id_store,
    a.id as id_article,
    79 as price,
    50 as amount,
    CURRENT_TIMESTAMP,
    'chris'
FROM
    public.customer c
INNER JOIN public.store s
    ON 1 = 1
INNER JOIN public.article a
    ON 1 = 1
where s.nk_number = 311 and c.nk_number = 1234334
and a.nk_number = 121234
ON CONFLICT (nk)
DO
UPDATE SET id_customer = EXCLUDED.id_customer, id_store = EXCLUDED.id_store, id_article = EXCLUDED.id_article,
    price = EXCLUDED.price, amount = EXCLUDED.amount, created_by = EXCLUDED.created_by
;

----------

INSERT
    INTO public.sale
        (nk, id_customer, id_store, id_article, price, amount, sale_date, created_by)
SELECT
    '12334',
    c.id as id_customer,
    s.id as id_store,
    a.id as id_article,
    69 as price,
    100 as amount,
    CURRENT_TIMESTAMP,
    'chris'
FROM
    public.customer c
INNER JOIN public.store s
    ON 1 = 1
INNER JOIN public.article a
    ON 1 = 1
where s.nk_number = 311 and c.nk_number = 1234334
and a.nk_number = 121234
ON CONFLICT (nk)
DO
UPDATE SET id_customer = EXCLUDED.id_customer, id_store = EXCLUDED.id_store, id_article = EXCLUDED.id_article,
    price = EXCLUDED.price, amount = EXCLUDED.amount, created_by = EXCLUDED.created_by
;

----------

INSERT
    INTO public.sale
        (nk, id_customer, id_store, id_article, price, amount, sale_date, created_by)
SELECT
    '1454546',
    c.id as id_customer,
    s.id as id_store,
    a.id as id_article,
    49 as price,
    9999 as amount,
    CURRENT_TIMESTAMP,
    'chris'
FROM
    public.customer c
INNER JOIN public.store s
    ON 1 = 1
INNER JOIN public.article a
    ON 1 = 1
where s.nk_number = 511 and c.nk_number = 2121333
and a.nk_number = 92833
ON CONFLICT (nk)
DO
UPDATE SET id_customer = EXCLUDED.id_customer, id_store = EXCLUDED.id_store, id_article = EXCLUDED.id_article,
    price = EXCLUDED.price, amount = EXCLUDED.amount, created_by = EXCLUDED.created_by
;

----------

INSERT
    INTO public.sale
        (nk, id_customer, id_store, id_article, price, amount, sale_date, created_by)
SELECT
    '129349344',
    c.id as id_customer,
    s.id as id_store,
    a.id as id_article,
    10000 as price,
    1 as amount,
    CURRENT_TIMESTAMP,
    'chris'
FROM
    public.customer c
INNER JOIN public.store s
    ON 1 = 1
INNER JOIN public.article a
    ON 1 = 1
where s.nk_number = 311 and c.nk_number = 8856565
and a.nk_number = 98383
ON CONFLICT (nk)
DO
UPDATE SET id_customer = EXCLUDED.id_customer, id_store = EXCLUDED.id_store, id_article = EXCLUDED.id_article,
    price = EXCLUDED.price, amount = EXCLUDED.amount, created_by = EXCLUDED.created_by
;

----------

INSERT
    INTO public.sale
        (nk, id_customer, id_store, id_article, price, amount, sale_date, created_by)
SELECT
    '1343556456',
    c.id as id_customer,
    s.id as id_store,
    a.id as id_article,
    9999 as price,
    1 as amount,
    CURRENT_TIMESTAMP,
    'chris'
FROM
    public.customer c
INNER JOIN public.store s
    ON 1 = 1
INNER JOIN public.article a
    ON 1 = 1
where s.nk_number = 511 and c.nk_number = 3435555
and a.nk_number = 92833
ON CONFLICT (nk)
DO
UPDATE SET id_customer = EXCLUDED.id_customer, id_store = EXCLUDED.id_store, id_article = EXCLUDED.id_article,
    price = EXCLUDED.price, amount = EXCLUDED.amount, created_by = EXCLUDED.created_by
;
