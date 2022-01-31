import os
import stat
import logging
from datetime import datetime
from pprint import pformat

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from jinja2 import Template

DAG_PATH = '/opt/airflow/dags'


def write_dag_file(file_name, content):
    """ Write DAG file """
    fqpn = os.path.join(DAG_PATH, file_name + ".py")

    with open(fqpn, 'w') as f:
        f.write(content)

    st = os.stat(fqpn)
    os.chmod(fqpn, st.st_mode | stat.S_IEXEC)


def generate_dags():
    """ Generate DAG code from metadata """

    log: logging.log = logging.getLogger("airflow")
    log.setLevel(logging.INFO)

    # Get active DAGs
    q_get_dags = """
  with dag_active as (
  select
      distinct dt.id_dag as id_dag
  from meta.dag_task dt
      where dt.active = true
  )
  select
      d.id as id_dag,
      d.dag_id,
      d.dag_start_date,
      t.template_src as template_src_dag,
      s.execute_at,
      t.nk
  from
      dag_active da
  inner join meta.dag d
      on da.id_dag = d.id
  inner join meta.template t
      on t.id = d.id_template
  inner join meta.schedule s
      on d.id = s.id_dag
 """

    # Get tasks by DAG id
    q_get_tasks = """
select
    dt.id_task,
    t.name as task_name,
    dt.airflow_conn,
    cmd,
    tt.template_src as template_src_task,
    tt.nk
from meta.dag_task dt
inner join meta.dag d
    on d.id = dt.id_dag
inner join meta.task t
    on dt.id_task = t.id
inner join meta.template tt
    on t.id_template = tt.id
where
    dt.id_dag = %(id_dag)s
"""

    # Get task dependencies by DAG id
    q_get_seq = """
WITH node
     AS (SELECT dt.id  AS id_dag_task,
                t.id   AS id_task,
                d.id   AS id_dag,
                t.NAME AS task_name
         FROM   meta.dag_task dt
                INNER JOIN meta.dag d
                        ON d.id = dt.id_dag
                INNER JOIN meta.task t
                        ON t.id = dt.id_task)
SELECT node_parent.task_name parent,
       node_child.task_name  AS child
FROM   meta.seq_dag_task child
       INNER JOIN meta.seq_dag_task parent
               ON parent.child = child.child
       INNER JOIN node node_parent
               ON node_parent.id_dag_task = parent.parent
       INNER JOIN node node_child
               ON node_child.id_dag_task = parent.child
WHERE  parent.depth != 0
       AND node_parent.id_dag = %(id_dag)s
       AND child.parent = (SELECT MIN(id_dag_task)
                           FROM   node n
                                  INNER JOIN meta.seq_dag_task s
                                          ON s.parent = n.id_dag_task
                           WHERE  n.id_dag = %(id_dag)s
                                  AND s.depth = 0)
"""

    postgres_hook = PostgresHook(postgres_conn_id='etl_pg')
    dags = postgres_hook.get_pandas_df(sql=q_get_dags)

    for idx_dag, active_dag in dags.iterrows():
        rendered_tasks = []

        q_get_tasks_parameters = {"id_dag": active_dag['id_dag']}
        tasks = postgres_hook.get_pandas_df(sql=q_get_tasks,
                                            parameters=q_get_tasks_parameters)

        for idx_task, task in tasks.iterrows():
            task_data = {
                'task_id': task['task_name'],
                'task_conn': task['airflow_conn'],
                'cmd': task['cmd']}

            log.info('Template: ' + task['nk'])
            log.info('Metadata: ' + pformat(task_data))

            raw_task_template = task['template_src_task']

            compiled_task_template = Template(raw_task_template, trim_blocks=True)
            rendered_task = compiled_task_template.render(task_data)
            rendered_tasks.append(rendered_task)

        q_get_seq_parameters = {"id_dag": active_dag['id_dag']}
        dependencies = postgres_hook.get_pandas_df(sql=q_get_seq,
                                                   parameters=q_get_seq_parameters)

        dependencies_src = []
        for idx_dep, dep in dependencies.iterrows():
            dependencies_src.append(dep['parent'] + ' >> ' + dep['child'])

        log.info('Dependencies: ' + pformat(dependencies_src))

        dag_data = {
            'dag_id': active_dag['dag_id'],
            'start_date': active_dag['dag_start_date'],
            'schedule_interval': active_dag['execute_at'],
            'tasks': [],
            'dependencies': []}

        log.info('Template: ' + active_dag['nk'])
        log.info('Metadata: ' + pformat(dag_data))

        dag_data['tasks'].extend(rendered_tasks)
        dag_data['dependencies'].extend(dependencies_src)

        dag_templ_raw = active_dag['template_src_dag']
        compiled_dag_templ = Template(dag_templ_raw, trim_blocks=True)

        rendered_dag = compiled_dag_templ.render(dag_data)

        write_dag_file(active_dag['dag_id'], rendered_dag)


with DAG(
        "generator",
        start_date=datetime(2021, 10, 1),
        max_active_runs=1,
        schedule_interval=None,
) as dag:
    t1 = PythonOperator(
        task_id="generate_dags",
        python_callable=generate_dags,
    )
