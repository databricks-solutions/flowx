from airflow.decorators import dag, task
@dag(dag_id="tf", schedule="0 6 * * *")
def pipeline():
    @task
    def extract():
        return [1,2,3]
    @task
    def transform(data):
        return sum(data)
    @task
    def load(total):
        print(total)
    load(transform(extract()))
pipeline()
