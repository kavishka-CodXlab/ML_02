from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('telco_batch_pipeline', start_date=datetime(2025,1,1), schedule_interval='@daily', catchup=False) as dag:
    produce = BashOperator(
        task_id='produce_batch',
        bash_command='python /opt/airflow/scripts/producer.py --mode batch --csv /opt/airflow/data/Telco-Customer-Churn.csv --batch-size 500'
    )
    consume = BashOperator(
        task_id='consume_batch',
        bash_command='python /opt/airflow/scripts/consumer.py --mode batch --window 1000 --model /opt/airflow/model.joblib'
    )
    summary = BashOperator(
        task_id='summary',
        bash_command='python /opt/airflow/scripts/generate_summary.py'
    )
    produce >> consume >> summary
