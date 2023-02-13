from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from clinical_trials.kafka_producer import send_to_kafka
from clinical_trials.kafka_consumer import extract_from_kafka

topic, key = 'ct_studies', 'ct_studies_key'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}


dag = DAG(
    'kafka_to_csv_dag',
    default_args=default_args,
    description='A DAG to demonstrate reading from a Kafka topic and writing to a CSV file using Apache Airflow and Amazon Managed Apache Airflow',
    schedule_interval=timedelta(seconds=30),
)

csv_to_kafka_task = PythonOperator(
    task_id='csv_to_kafka',
    python_callable=send_to_kafka,
    dag=dag
)

# kafka_to_csv_task = PythonOperator(
#     task_id='kafka_to_csv',
#     python_callable=extract_from_kafka,
#     dag=dag
# )


csv_to_kafka_task
