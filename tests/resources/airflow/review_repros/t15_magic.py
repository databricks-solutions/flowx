from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="magic", schedule_interval="0 6 * * *") as dag:
    a = BashOperator(task_id="multi", bash_command="""
set -e
echo "quoted 'inner' \"esc\""
python -c 'print("hi")'
# MAGIC %sql
aws s3 cp a s3://b/c
""")
