# ============================================================
# CineVerse | common.py
# Shared hooks, connection checks, schema prep, Spark session,
# and the dynamic catalog helpers used by all sources.
# ============================================================
import json
import logging
import random
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from cineverse.config import (
    MINIO_CONN_ID, POSTGRES_CONN_ID, BUCKET_NAME, TARGET_SCHEMAS,
    GOLD_SCHEMA, POSTGRES_JDBC_JAR, BASE_MOVIES, BASE_USERS, NEW_PER_RUN,
    STATE_KEY, CATALOG_KEY,
)

log = logging.getLogger(__name__)


# ---------------- hooks ----------------
def get_pg():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def get_s3():
    return S3Hook(aws_conn_id=MINIO_CONN_ID)


def ensure_bucket():
    s3 = get_s3()
    if not s3.check_for_bucket(BUCKET_NAME):
        s3.create_bucket(bucket_name=BUCKET_NAME)


# ---------------- connection checks ----------------
def check_minio_connection(**_):
    get_s3().get_conn().list_buckets()
    log.info("MinIO connection OK")


def check_postgres_connection(**_):
    if get_pg().get_first("SELECT 1;")[0] != 1:
        raise RuntimeError("PostgreSQL connection test failed")
    log.info("PostgreSQL connection OK")


# ---------------- schema prep ----------------
def prepare_postgres_schema(**_):
    pg = get_pg()
    for schema in TARGET_SCHEMAS:
        pg.run(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    pg.run(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.etl_audit_log (
            id SERIAL PRIMARY KEY, dataset TEXT, layer TEXT, table_name TEXT,
            total_rows INTEGER, status TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    """)
    log.info("Schemas ready: %s", TARGET_SCHEMAS)


def audit(pg, dataset, layer, table, rows, status="OK"):
    pg.run(f"""INSERT INTO {GOLD_SCHEMA}.etl_audit_log (dataset, layer, table_name, total_rows, status)
               VALUES (%s,%s,%s,%s,%s)""",
           parameters=(dataset, layer, table, int(rows), status))


# ---------------- Spark (local mode + Postgres JDBC driver) ----------------
def get_spark(app):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .appName(app)
             .master("local[*]")
             .config("spark.jars", POSTGRES_JDBC_JAR)
             .config("spark.driver.extraClassPath", POSTGRES_JDBC_JAR)
             .config("spark.executor.extraClassPath", POSTGRES_JDBC_JAR)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    return spark


def jdbc_params():
    """Build (url, properties) for Spark <-> Postgres over JDBC."""
    c = get_pg().get_connection(POSTGRES_CONN_ID)
    port = c.port or 5432
    db = c.schema
    if not db:
        raise ValueError("Database name missing in the Airflow Postgres connection")
    url = f"jdbc:postgresql://{c.host}:{port}/{db}"
    props = {"user": c.login, "password": c.password, "driver": "org.postgresql.Driver"}
    return url, props


# ---------------- dynamic catalog (grows every run) ----------------
_ADJ = ["The Last", "Broken", "Silent", "Crimson", "Eternal",
        "Hidden", "Lost", "Golden", "Dark", "Wild"]
_NOUN = ["Horizon", "Empire", "Shadow", "Voyage", "Legacy",
         "Signal", "Kingdom", "Echo", "Paradox", "Dawn"]


def _title(i):
    return f"{_ADJ[i % len(_ADJ)]} {_NOUN[(i // len(_ADJ)) % len(_NOUN)]} {i}"


def prepare_catalog(**_):
    """Grow catalog + user base by NEW_PER_RUN each run (tied to run count via a
    small state file in MinIO), then publish the title list all sources read."""
    ensure_bucket()
    s3 = get_s3()
    try:
        st = json.loads(s3.read_key(STATE_KEY, bucket_name=BUCKET_NAME))
        movies = int(st["movies"]) + NEW_PER_RUN
        users = int(st["users"]) + NEW_PER_RUN
    except Exception:
        movies, users = BASE_MOVIES, BASE_USERS
    s3.load_string(json.dumps({"movies": movies, "users": users}),
                   key=STATE_KEY, bucket_name=BUCKET_NAME, replace=True)
    titles = [_title(i) for i in range(1, movies + 1)]
    s3.load_string(json.dumps({"titles": titles, "users": users}),
                   key=CATALOG_KEY, bucket_name=BUCKET_NAME, replace=True)
    log.info("catalog this run: %d movies, %d users", movies, users)


def get_catalog():
    data = json.loads(get_s3().read_key(CATALOG_KEY, bucket_name=BUCKET_NAME))
    return data["titles"], int(data["users"])


# ---------------- shared dirty-data helpers ----------------
_FMTS = ["%Y-%m-%d", "%d/%m/%Y", "%B %d, %Y", "%m-%d-%Y", None]
_GENRES = ["Action", "Drama", "Sci-Fi", "Comedy", "Horror", "Thriller", "Romance"]
_COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "Japan", "Brazil", "India"]


def dirty_date():
    dt = datetime(random.randint(2016, 2024), random.randint(1, 12), random.randint(1, 28))
    f = random.choice(_FMTS)
    return dt.strftime(f) if f else None


def dirty_case(v):
    return random.choice([v, v.lower(), v.upper(), f" {v} "])
