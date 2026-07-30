# ============================================================
# CineVerse | config.py
# Central configuration (connections, storage, schemas, tables)
# ============================================================

# ---------------- CONNECTIONS ----------------
MINIO_CONN_ID = "minio_conn"
POSTGRES_CONN_ID = "postgres_conn"

# ---------------- STORAGE (MinIO) ----------------
BUCKET_NAME = "nilufar"
MINIO_PREFIX = {
    "api": "CineVerse/API/",       # raw movie catalog  (JSON)
    "file": "CineVerse/File/",     # raw CSV / JSON / XML feeds
    "db": "CineVerse/DB/",         # raw operational data (PARQUET)
}

# ---------------- POSTGRES SCHEMAS (medallion) ----------------
BRONZE_SCHEMA = "bronze_nilufar"
SILVER_SCHEMA = "silver_nilufar"
GOLD_SCHEMA = "gold_nilufar"
TARGET_SCHEMAS = [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]

# ---------------- SPARK ----------------
POSTGRES_JDBC_JAR = "/opt/spark/jars/postgresql-42.7.3.jar"

# ---------------- CATALOG GROWTH (dynamic) ----------------
BASE_MOVIES = 100
BASE_USERS = 100
NEW_PER_RUN = 20                        # new movies + users released each run
STATE_KEY = "CineVerse/_state/catalog_state.json"
CATALOG_KEY = "CineVerse/_state/catalog_titles.json"

# ---------------- TABLES (bronze / silver / gold per dataset) ----------------
# Each dataset flows: MinIO raw -> bronze -> silver -> gold (all in Postgres).
TABLES = {
    "movies":    {"source": "api",  "bronze": "movies",    "silver": "movies",    "gold": "genre_summary"},
    "scores":    {"source": "file", "bronze": "scores",    "silver": "scores",    "gold": "score_summary"},
    "watchlist": {"source": "file", "bronze": "watchlist", "silver": "watchlist", "gold": "title_popularity"},
    "awards":    {"source": "file", "bronze": "awards",    "silver": "awards",    "gold": "awards_won"},
    "users":     {"source": "db",   "bronze": "users",     "silver": "users",     "gold": "users_by_country"},
    "ratings":   {"source": "db",   "bronze": "ratings",   "silver": "ratings",   "gold": "fact_ratings"},
    "reviews":   {"source": "db",   "bronze": "reviews",   "silver": "reviews",   "gold": "review_summary"},
}
