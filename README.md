# 🎬 CineVerse — Data Engineering Pipeline

An end-to-end pipeline for a movies & series platform: three sources → Bronze/Silver/Gold → dashboard.


## Stack
Apache Airflow · MinIO · PySpark · PostgreSQL · Apache Superset

## How it works
Sources land raw in **MinIO** → **Bronze** (Postgres) → cleaned into **Silver** with PySpark →
aggregated into **Gold** with SQL → shown in **Superset**. One modular Airflow DAG runs it hourly,
and the data grows automatically every run.

**Sources:** API (JSON) · Files (CSV/JSON/XML) · Database (Parquet)

## Structure

dags/
├── cineverse_main_dag.py     # the DAG (imports + wiring)
└── cineverse/                # logic package
    ├── config.py   common.py   cleaning.py
    ├── api.py      file.py     db.py
    └── silver.py   gold.py


## Run
1. Copy `cineverse_main_dag.py` + the `cineverse/` folder into your Airflow `dags/`.
2. Set connections `minio_conn` and `postgres_conn`.
3. Trigger `cineverse_main_dag`, then query `gold_nilufar.movie_360`.

## Key tables (gold_nilufar)
`movie_360` (main mart) · `fact_ratings` · `genre_summary` · `users_by_country` ·
`title_popularity` · `awards_won` · `review_summary` · `run_metrics`

Built by **Nilufar Mammadova**.
