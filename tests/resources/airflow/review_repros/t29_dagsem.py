from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="dsem", schedule_interval="0 6 * * *", max_active_runs=1, max_active_tasks=4,
         default_args={"depends_on_past": True, "wait_for_downstream": True, "sla": None}) as dag:
    a = BashOperator(task_id="a", bash_command="echo a", pool="critical", priority_weight=10, queue="high")
