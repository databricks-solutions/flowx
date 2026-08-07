from airflow import DAG
from airflow.operators.bash import BashOperator
dag_a = DAG(dag_id="team_a_etl", schedule_interval="0 3 * * *")
a1 = BashOperator(task_id="extract", bash_command="echo a1", dag=dag_a)
a2 = BashOperator(task_id="load", bash_command="echo a2", dag=dag_a)
a1 >> a2
dag_b = DAG(dag_id="team_b_etl", schedule_interval="0 9 * * *")
b1 = BashOperator(task_id="extract", bash_command="echo b1", dag=dag_b)
b2 = BashOperator(task_id="load", bash_command="echo b2", dag=dag_b)
b1 >> b2
