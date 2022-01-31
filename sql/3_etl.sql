\c etl;

CREATE SCHEMA meta;

CREATE TABLE meta.template (
    id serial PRIMARY KEY,
    nk VARCHAR (255) NOT null,
    type VARCHAR (255) NOT NULL,
    template_src text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);
CREATE UNIQUE INDEX idx_template_nk ON meta.template (nk);

CREATE TABLE meta.dag (
    id serial PRIMARY KEY,
    dag_id VARCHAR (255) NOT NULL,
    dag_start_date VARCHAR (255) not null,
    id_template BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);
CREATE UNIQUE INDEX idx_dag_dag_id ON meta.dag (dag_id);

CREATE TABLE meta.task (
    id serial PRIMARY KEY,
    name VARCHAR (255) NOT NULL,
    id_template BIGINT,
    cmd VARCHAR (1025) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);
CREATE UNIQUE INDEX idx_task_name ON meta.task (name);

CREATE TABLE meta.dag_task (
    id serial PRIMARY KEY,
    id_dag bigint,
    id_task bigint,
    nk VARCHAR (255) NOT NULL,
    airflow_conn VARCHAR(255),
    config json,
    active boolean NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);
CREATE UNIQUE INDEX idx_dag_task_nk ON meta.dag_task (nk);

CREATE TABLE meta.seq_dag_task (
    id serial PRIMARY KEY,
    nk VARCHAR (255) NOT NULL,
    parent bigint NOT NULL default 0,
    child bigint NOT NULL default 0,
    depth bigint NOT NULL default 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);
CREATE UNIQUE INDEX idx_seq_dag_task_nk ON meta.seq_dag_task (nk);

CREATE TABLE meta.schedule (
    id serial PRIMARY KEY,
    name VARCHAR (255) NOT NULL,
    id_dag bigint,
    active boolean NOT NULL DEFAULT TRUE,
    execute_at VARCHAR (255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);

CREATE UNIQUE INDEX idx_schedule_name ON meta.schedule (name);

-----------------------
-----------------------


INSERT INTO meta.template (nk, type, template_src, created_by)
values ('simple_dag', 'dag',

'import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="{{dag_id}}",
    start_date=datetime.datetime{{start_date}},
    schedule_interval="{{schedule_interval}}",
    catchup=False,
) as dag:
{% for task in tasks %}
{{task}}
{% endfor %}

{% for dependency in dependencies %}
{{dependency}}
{% endfor %}

'

, 'chris'
),
('execute_sp', 'task',

'    {{task_id}} = PostgresOperator(
        task_id="{{task_id}}",
        postgres_conn_id="{{task_conn}}",
        sql="{{cmd}}",
        autocommit = True,
    )
'

, 'chris'
)
ON CONFLICT (nk)
DO
   UPDATE SET type = EXCLUDED.type, template_src = EXCLUDED.template_src, created_by = EXCLUDED.created_by
   ;

------------------------------------------


INSERT INTO meta.dag (dag_id, dag_start_date, id_template, created_by)
VALUES ('load_sale_star', '(2020, 2, 2)', -1, 'chris'),
       ('CALCULATE', '(2020, 2, 2)', -1, 'chris')
ON CONFLICT (dag_id)
DO
   UPDATE SET id_template = EXCLUDED.id_template, created_by = EXCLUDED.created_by
   ;

UPDATE meta.dag set id_template =
    (SELECT id from meta.template where nk = 'simple_dag');


--------------------------------------------------

INSERT INTO meta.task (name, id_template, cmd, created_by)
VALUES  ('load_dim_customer', -1, 'CALL load.sp_dim_customer();', 'chris'),
        ('load_dim_article', -1, 'CALL load.sp_dim_article();', 'chris'),
        ('load_dim_store', -1, 'CALL load.sp_dim_store();', 'chris'),
        ('load_fact_sale', -1, 'CALL load.sp_fact_sale();', 'chris')
ON CONFLICT (name)
DO
    UPDATE SET id_template = EXCLUDED.id_template, created_by = EXCLUDED.created_by
    ;

UPDATE meta.task set id_template =
    (SELECT id from meta.template where nk = 'execute_sp');

--------------------------------------------------

INSERT
    INTO meta.dag_task
        (id_dag, id_task, nk, airflow_conn, created_by)
SELECT
    d.id, t.id, d.id::char || '.' || t.id::char, 'dwh_pg' AS airflow_conn, 'chris'
FROM
    meta.dag d
INNER JOIN meta.task t ON
    1 = 1
where d.dag_id = 'load_sale_star' and t.name in ('load_dim_customer', 'load_dim_article', 'load_dim_store', 'load_fact_sale')
ON CONFLICT (nk)
DO
    UPDATE SET id_dag = EXCLUDED.id_dag, id_task = EXCLUDED.id_task,
    airflow_conn = EXCLUDED.airflow_conn, created_by = EXCLUDED.created_by
    ;

--------------------------------------------------
-- 'roots'
INSERT
    INTO meta.seq_dag_task
        (nk, parent, child, created_by)
SELECT
    d.id::char || '.' || t.id::char || '.' || t.id::char as nk,
    dt.id as parent,
    dt.id as child,
    'chris' as created_by
FROM
    meta.dag_task dt
INNER JOIN meta.task t ON
    t.id = dt.id_task
INNER JOIN meta.dag d ON
    d.id = dt.id_dag
where d.dag_id = 'load_sale_star'
ON CONFLICT (nk)
DO
    UPDATE SET parent = EXCLUDED.parent, child = EXCLUDED.child,
    created_by = EXCLUDED.created_by
    ;

---------------------------------------------------

INSERT
    INTO meta.seq_dag_task
        (nk, parent, child, depth, created_by)
with filter as (
    select
        dt.id,
        d.dag_id,
        t.name
    from meta.dag_task dt
    inner join meta.dag d on
        d.id = dt.id_dag
    inner join meta.task t on
        t.id = dt.id_task
)
SELECT
    d.id::char || '.' || a.parent::char || '.' || d.child::char,
    a.parent,
    d.child,
    a.depth + d.depth + 1,
    'chris'
FROM meta.seq_dag_task a
CROSS join meta.seq_dag_task d
inner join filter f on
    f.id = a.parent and f.dag_id = 'load_sale_star'
inner join filter f_a on
    f_a.id = d.child and f_a.id != a.parent and f_a.name = 'load_fact_sale'
WHERE a.depth = 0 and d.depth = 0
ON CONFLICT (nk)
DO
    UPDATE SET parent = EXCLUDED.parent, child = EXCLUDED.child, created_by = EXCLUDED.created_by
;

--------------------------------------------------

INSERT
    INTO meta.schedule (id_dag, name, execute_at, created_by)
SELECT
    d.id as id_dag, 'nightly', '59 23 * * *', 'chris'
FROM
    meta.dag d
where d.dag_id = 'load_sale_star'
ON CONFLICT (name)
DO
    UPDATE SET id_dag = EXCLUDED.id_dag, execute_at = EXCLUDED.execute_at, created_by = EXCLUDED.created_by
;

