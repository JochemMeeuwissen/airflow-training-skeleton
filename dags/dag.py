import datetime as dt

from airflow import DAG
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from operators.http_gcs import HttpToGcsOperator

dag = DAG(
    dag_id="my_third_dag",
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


for currency in {'EUR', 'USD'}:
    HttpToGcsOperator(
    task_id="get_currency_" + currency,
    method="GET",
    endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=" + currency,
    http_conn_id="airflow-training-currency-http",
    gcs_conn_id="airflow-training-storage-bucket",
    bucket="airflow-training-knab-jochem"
    gcs_path="currency/{{ ds }}-" + currency + ".json",
    dag=dag,
)