# ============================================================
# CineVerse | db.py  (DB source: users, ratings, reviews)
# upload_db_to_minio : operational data written as PARQUET in MinIO
# load_db_bronze     : parquet -> bronze_nilufar.{users,ratings,reviews}
# ============================================================
import io
import logging
import random

import pandas as pd

from cineverse.common import (get_s3, get_pg, ensure_bucket, get_catalog,
                              dirty_date, dirty_case, _COUNTRIES, audit)
from cineverse.config import BUCKET_NAME, MINIO_PREFIX, BRONZE_SCHEMA, TABLES

log = logging.getLogger(__name__)
P = MINIO_PREFIX["db"]
_SENTIMENTS = ["positive", "POSITIVE", " negative ", "Neutral", None]
_BODIES = [" loved it ", "meh", "MASTERPIECE", None, "not my taste", "great watch"]


def _put_parquet(df, key):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    get_s3().load_bytes(buf.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
    log.info("DB raw parquet -> s3://%s/%s (%d rows)", BUCKET_NAME, key, len(df))


def upload_db_to_minio(**_):
    """Generate dirty users/ratings/reviews and store them as Parquet in MinIO."""
    ensure_bucket()
    titles, users = get_catalog()

    # users (grow with catalog)
    urows = [{"user_id": i,
              "username": dirty_case(f"user{i}"),
              "email": f"user{i}@cineverse.io",
              "country": dirty_case(random.choice(_COUNTRIES)),
              "signup_date": dirty_date(),
              "is_premium": random.choice(["true", "1", "yes", "false", "0", None])}
             for i in range(1, users + 1)]
    urows.append(dict(random.choice(urows)))                      # dirty duplicate email
    _put_parquet(pd.DataFrame(urows), f"{P}users.parquet")

    # ratings: one per movie + extras
    rrows, rid = [], 0
    for t in titles:
        rid += 1
        rrows.append({"rating_id": rid, "user_id": random.randint(1, users),
                      "movie_title": dirty_case(t),
                      "score": str(round(random.uniform(1, 5), 1)),
                      "watched_date": dirty_date(), "review": random.choice(_BODIES)})
    for _ in range(20):
        rid += 1
        rrows.append({"rating_id": rid, "user_id": random.randint(1, users),
                      "movie_title": dirty_case(random.choice(titles)),
                      "score": random.choice(["four", "999", None, "3"]),
                      "watched_date": dirty_date(), "review": "x"})
    _put_parquet(pd.DataFrame(rrows), f"{P}ratings.parquet")

    # reviews: one per ~half the movies
    vrows, vid = [], 0
    for t in random.sample(titles, k=max(1, len(titles) // 2)):
        vid += 1
        vrows.append({"review_id": vid, "user_id": random.randint(1, users),
                      "movie_title": dirty_case(t),
                      "sentiment": random.choice(_SENTIMENTS),
                      "body": random.choice(_BODIES), "created_date": dirty_date()})
    _put_parquet(pd.DataFrame(vrows), f"{P}reviews.parquet")


def _read_parquet(key):
    obj = get_s3().get_key(key, bucket_name=BUCKET_NAME)
    return pd.read_parquet(io.BytesIO(obj.get()["Body"].read()))


def _to_bronze(df, dataset):
    pg = get_pg()
    tbl = TABLES[dataset]["bronze"]
    df = df.astype(object).where(lambda x: x.notna(), None)
    df.to_sql(tbl, pg.get_sqlalchemy_engine(), schema=BRONZE_SCHEMA,
              if_exists="replace", index=False, method="multi", chunksize=1000)
    audit(pg, dataset, "bronze", tbl, len(df))
    log.info("bronze %s.%s: %d rows", BRONZE_SCHEMA, tbl, len(df))


def load_db_bronze(**_):
    _to_bronze(_read_parquet(f"{P}users.parquet"), "users")
    _to_bronze(_read_parquet(f"{P}ratings.parquet"), "ratings")
    _to_bronze(_read_parquet(f"{P}reviews.parquet"), "reviews")
