from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="ss_dag", schedule_interval="0 6 * * *") as dag:
    a = BashOperator(task_id="submit_mem", bash_command="spark-submit --master yarn --executor-memory 4g --num-executors 10 /jobs/etl.py --date 2024-01-01")
    b = BashOperator(task_id="submit_cd", bash_command="cd /opt/app && spark-submit /jobs/other.py")
    c = BashOperator(task_id="submit_and", bash_command="spark-submit /jobs/x.py && aws s3 cp out s3://b/o")
    a >> b >> c
