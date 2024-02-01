from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreatePysparkJobOperator,
)


# Common settings for your environment
YC_DP_CLUSTER_ID = "c9ql8pib25f4g4g0i2ov"


# Settings for S3 buckets
YC_SOURCE_BUCKET = "mlopsshakhova"  # YC S3 bucket for pyspark source files

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
    dag_id="DATA_PREPROCESS_3",
    # schedule_interval='@hourly',
    start_date=datetime(year=2024, month=1, day=30),
    schedule_interval=timedelta(minutes=70),
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
        task_id="dp-cluster-model-train-valid-task",
        main_python_file_uri=f"s3a://{YC_SOURCE_BUCKET}/scripts/model_train_valid.py",
        connection_id=ycSA_connection.conn_id,
        cluster_id=YC_DP_CLUSTER_ID,
        dag=ingest_dag,
    )

    # Формирование DAG из указанных выше этапов
data_processing >> model_training
