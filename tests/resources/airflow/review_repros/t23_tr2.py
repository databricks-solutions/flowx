from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
with DAG(dag_id="tr2", schedule_interval="0 6 * * *") as dag:
    up = BashOperator(task_id="up", bash_command="echo u")
    ns = BashOperator(task_id="ns", bash_command="echo n", trigger_rule="none_skipped")
    asr = BashOperator(task_id="asr", bash_command="echo s", trigger_rule="all_skipped")
    od = BashOperator(task_id="od", bash_command="echo d", trigger_rule="one_done")
    tr = BashOperator(task_id="tr", bash_command="echo t", trigger_rule=TriggerRule.ALL_DONE)
    for t in (ns, asr, od, tr): up >> t
