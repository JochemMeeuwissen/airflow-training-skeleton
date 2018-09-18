import datetime as dt

from airflow import DAG
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator


dag = DAG(
    dag_id="my_second_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 9, 11),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


def print_exec_date(**context):
    print(context["execution_date"])


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    postgres_conn_id="airflow-training-postgres",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="airflow-training-knab-jochem",
    filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
    dag=dag,
)
