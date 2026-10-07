from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="jinja_dag", schedule_interval="0 6 * * *") as dag:
    a = BashOperator(task_id="nodash", bash_command="run.sh --d {{ ds_nodash }} --w {{ macros.ds_add(ds, -7) }} --x {{ ti.xcom_pull(task_ids='u') }}")
