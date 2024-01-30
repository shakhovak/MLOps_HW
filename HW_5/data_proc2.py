import uuid
from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)


# Common settings for your environment
YC_DP_FOLDER_ID = "b1gsqum16i8mi1516107"
YC_DP_SUBNET_ID = "e9bk4319fjbmq6rudpiq"
YC_DP_SA_ID = "aje4pp9e6dmrvtj6k5qg"
YC_DP_AZ = "ru-central1-a"
YC_DP_SSH_PUBLIC_KEY = Variable.get("SSH_PUBLIC")
YC_DP_GROUP_ID = "enpv65jq6ep903597nmr"
YC_DP_CLUSTER_ID = 'c9qedvq3qjhjsbfgsqt4'


# Settings for S3 buckets
YC_INPUT_DATA_BUCKET = "mlopsshakhova/airflow/"  # YC S3 bucket for input data
YC_SOURCE_BUCKET = "mlopsshakhova"  # YC S3 bucket for pyspark source files
YC_DP_LOGS_BUCKET = (
    "mlopsshakhova/airflow_logs/"  # YC S3 bucket for Data Proc cluster logs
)


# Создание подключения для Object Storage
session = settings.Session()
ycS3_connection = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host="https://storage.yandexcloud.net/",
    extra={
        "aws_access_key_id": Variable.get("S3_KEY_ID"),
        "aws_secret_access_key": Variable.get("S3_SECRET_KEY"),
        "host": "https://storage.yandexcloud.net/",
    },
)


if (
    not session.query(Connection)
    .filter(Connection.conn_id == ycS3_connection.conn_id)
    .first()
):
    session.add(ycS3_connection)
    session.commit()

ycSA_connection = Connection(
    conn_id="yc-SA",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": Variable.get("DP_PUBLIC_SSH_KEY"),
        "extra__yandexcloud__service_account_json_path": Variable.get("DP_SA_PATH"),
    },
)

if (
    not session.query(Connection)
    .filter(Connection.conn_id == ycSA_connection.conn_id)
    .first()
):
    session.add(ycSA_connection)
    session.commit()


# Настройки DAG
with DAG(
    dag_id="DATA_PREPROCESS_2",
    # schedule_interval='@hourly',
    start_date=datetime(year=2024, month=1, day=30),
    schedule_interval=timedelta(minutes=40),
    catchup=False,
) as ingest_dag:


    # 2 этап: запуск задания PySpark
    data_processing = DataprocCreatePysparkJobOperator(
        task_id="dp-cluster-data-preprocess-task",
        main_python_file_uri=f"s3a://{YC_SOURCE_BUCKET}/scripts/pyspark_script.py",
        connection_id=ycSA_connection.conn_id,
        cluster_id=YC_DP_CLUSTER_ID,
        dag=ingest_dag,
    )

    model_training = DataprocCreatePysparkJobOperator(
        task_id="dp-cluster-model-train-task",
        main_python_file_uri=f"s3a://{YC_SOURCE_BUCKET}/scripts/model_train_fin.py",
        connection_id=ycSA_connection.conn_id,
        cluster_id=YC_DP_CLUSTER_ID,
        dag=ingest_dag,
    )


    # Формирование DAG из указанных выше этапов
data_processing >> model_training
