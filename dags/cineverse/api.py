# ============================================================
# CineVerse | api.py  (API source: movie catalog)
# upload_api_to_minio : simulated dynamic API -> raw JSON in MinIO
# load_api_bronze     : raw JSON -> bronze_nilufar.movies (Postgres)
# ============================================================
import json
import logging
import random

import pandas as pd

from cineverse.common import get_s3, get_pg, ensure_bucket, get_catalog, dirty_date, dirty_case, _GENRES, audit
from cineverse.config import BUCKET_NAME, MINIO_PREFIX, BRONZE_SCHEMA, TABLES

log = logging.getLogger(__name__)
KEY = f"{MINIO_PREFIX['api']}movies.json"


def upload_api_to_minio(**_):
    ensure_bucket()
    titles, _u = get_catalog()
    recs = []
    for i, title in enumerate(titles):
        rt = random.randint(70, 190)
        rr = round(random.uniform(1, 10), 1)
        recs.append({
            "id": 1000 + i,
            "title": dirty_case(title),
            "type": random.choice(["movie", "Movie", "MOVIE"]),
            "genre": dirty_case(_GENRES[i % len(_GENRES)]),
            "language": random.choice(["en", "EN", "English", None]),
            "release_date": dirty_date(),
            "runtime": random.choice([rt, f"{rt} min", str(rt), None]),
            "rating": random.choice([rr, str(rr), f"{rr}/10", "999"]),
            "votes": random.choice([random.randint(100, 900000), None]),
            "director": random.choice(["Chris Nolan", "BONG JOON-HO", "  Lana W ", None]),
        })
    for _ in range(15):
        recs.append(dict(random.choice(recs)))       # dirty duplicates
    random.shuffle(recs)
    get_s3().load_string(json.dumps(recs, default=str), key=KEY,
                         bucket_name=BUCKET_NAME, replace=True)
    log.info("API raw -> s3://%s/%s (%d rows)", BUCKET_NAME, KEY, len(recs))


def load_api_bronze(**_):
    pg = get_pg()
    body = get_s3().read_key(KEY, bucket_name=BUCKET_NAME)
    df = pd.DataFrame(json.loads(body)).astype(object).where(lambda x: x.notna(), None)
    tbl = TABLES["movies"]["bronze"]
    df.to_sql(tbl, pg.get_sqlalchemy_engine(), schema=BRONZE_SCHEMA,
              if_exists="replace", index=False, method="multi", chunksize=1000)
    audit(pg, "movies", "bronze", tbl, len(df))
    log.info("bronze %s.%s: %d rows", BRONZE_SCHEMA, tbl, len(df))
