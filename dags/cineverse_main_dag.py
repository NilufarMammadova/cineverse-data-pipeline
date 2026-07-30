# ============================================================
# CineVerse | MAIN DAG  (modular: logic lives in the cineverse/ package)
# Sources (API/File/DB) -> MinIO raw -> Bronze -> Silver(PySpark) -> Gold (all Postgres)
# ============================================================
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from cineverse.config import TABLES, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA
from cineverse.common import (
    check_minio_connection, check_postgres_connection,
    prepare_postgres_schema, prepare_catalog,
)
from cineverse.api import upload_api_to_minio, load_api_bronze
from cineverse.file import upload_file_to_minio, load_file_bronze
from cineverse.db import upload_db_to_minio, load_db_bronze
from cineverse.silver import load_to_silver
from cineverse.gold import load_to_gold, build_movie_360, record_run_metrics

default_args = {"owner": "Nilufar", "retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="cineverse_main_dag",
    default_args=default_args,
    description="CineVerse medallion pipeline (MinIO + PySpark + Postgres), modular, one DAG",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["cineverse", "minio", "pyspark", "postgres", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    check_minio = PythonOperator(task_id="check_minio", python_callable=check_minio_connection)
    check_postgres = PythonOperator(task_id="check_postgres", python_callable=check_postgres_connection)
    prepare_schema = PythonOperator(task_id="prepare_schema", python_callable=prepare_postgres_schema)
    prep_catalog = PythonOperator(task_id="prepare_catalog", python_callable=prepare_catalog)

    # ---- uploads (raw -> MinIO) ----
    upload_api = PythonOperator(task_id="upload_api_to_minio", python_callable=upload_api_to_minio)
    upload_file = PythonOperator(task_id="upload_file_to_minio", python_callable=upload_file_to_minio)
    upload_db = PythonOperator(task_id="upload_db_to_minio", python_callable=upload_db_to_minio)

    # ---- bronze (MinIO raw -> Postgres bronze) ----
    api_bronze = PythonOperator(task_id="load_api_bronze", python_callable=load_api_bronze)
    file_bronze = PythonOperator(task_id="load_file_bronze", python_callable=load_file_bronze)
    db_bronze = PythonOperator(task_id="load_db_bronze", python_callable=load_db_bronze)

    # ---- per-dataset silver + gold (built from the TABLES config) ----
    gold_tasks = []

    def make_silver_gold(ds):
        t = TABLES[ds]
        silver = PythonOperator(
            task_id=f"silver_{ds}", python_callable=load_to_silver,
            op_kwargs={"dataset": ds, "bronze_schema": BRONZE_SCHEMA, "bronze_table": t["bronze"],
                       "silver_schema": SILVER_SCHEMA, "silver_table": t["silver"]})
        gold = PythonOperator(
            task_id=f"gold_{ds}", python_callable=load_to_gold,
            op_kwargs={"dataset": ds, "silver_schema": SILVER_SCHEMA, "silver_table": t["silver"],
                       "gold_schema": GOLD_SCHEMA, "gold_table": t["gold"]})
        silver >> gold
        gold_tasks.append(gold)
        return silver

    s_movies = make_silver_gold("movies")
    s_scores = make_silver_gold("scores")
    s_watchlist = make_silver_gold("watchlist")
    s_awards = make_silver_gold("awards")
    s_users = make_silver_gold("users")
    s_ratings = make_silver_gold("ratings")
    s_reviews = make_silver_gold("reviews")

    marts = PythonOperator(task_id="build_movie_360", python_callable=build_movie_360)
    metrics = PythonOperator(task_id="record_run_metrics", python_callable=record_run_metrics)

    # ---- wiring ----
    start >> [check_minio, check_postgres]
    [check_minio, check_postgres] >> prepare_schema >> prep_catalog
    prep_catalog >> [upload_api, upload_file, upload_db]

    upload_api >> api_bronze >> s_movies
    upload_file >> file_bronze >> [s_scores, s_watchlist, s_awards]
    upload_db >> db_bronze >> [s_users, s_ratings, s_reviews]

    gold_tasks >> marts >> metrics >> end
    