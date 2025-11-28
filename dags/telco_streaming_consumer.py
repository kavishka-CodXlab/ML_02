from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('telco_streaming_consumer', start_date=datetime(2025,1,1), schedule_interval=None, catchup=False) as dag:
    stream_run = BashOperator(
        task_id='run_streaming_consumer',
        bash_command='python /opt/airflow/scripts/consumer.py --mode streaming --metrics-port 8000 --model /opt/airflow/model.joblib'
    )
