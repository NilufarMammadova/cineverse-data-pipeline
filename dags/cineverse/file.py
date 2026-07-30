# ============================================================
# CineVerse | file.py  (File source: CSV scores, JSON watchlist, XML awards)
# upload_file_to_minio : generate raw CSV/JSON/XML -> MinIO
# load_file_bronze     : raw files -> bronze_nilufar.{scores,watchlist,awards}
# ============================================================
import csv
import io
import json
import logging
import random
import uuid
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

import pandas as pd

from cineverse.common import get_s3, get_pg, ensure_bucket, get_catalog, dirty_date, dirty_case, audit
from cineverse.config import BUCKET_NAME, MINIO_PREFIX, BRONZE_SCHEMA, TABLES

log = logging.getLogger(__name__)
P = MINIO_PREFIX["file"]


def _put(key, body):
    get_s3().load_string(body, key=key, bucket_name=BUCKET_NAME, replace=True)


def upload_file_to_minio(**_):
    ensure_bucket()
    titles, users = get_catalog()

    # CSV : external scores
    scores = [{"title": dirty_case(t),
               "imdb_score": random.choice([str(round(random.uniform(1, 10), 1)), "", "N/A"]),
               "rt_score": random.choice([random.randint(1, 100), "", "95%"]),
               "source": random.choice(["imdb", "IMDB", " rotten_tomatoes ", "RT"]),
               "collected_at": dirty_date()} for t in titles]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["title", "imdb_score", "rt_score", "source", "collected_at"])
    w.writeheader()
    for r in scores:
        w.writerow(r)
    _put(f"{P}external_scores.csv", buf.getvalue())

# JSON : watchlist events — each movie gets a RANDOM number of events so
    # watchlist_adds varies per movie (makes the "Top Trending" chart meaningful)
    events = []
    for t in titles:
        for _ in range(random.randint(1, 40)):          # 1–40 events per movie
            events.append({
                "event_id": str(uuid.uuid4()),
                "user": dirty_case(f"user{random.randint(1, users)}"),
                "title": dirty_case(t),
                # weight 'add' higher than 'remove' so adds dominate
                "action": random.choice(["add", "ADD", "Add", "add", "add", " remove "]),
                "event_time": dirty_date(),
                "device": random.choice(["ios", "Android", "WEB", ""]),
            })
    _put(f"{P}watchlist_events.json", json.dumps({"events": events}, indent=2))

    # XML : awards (unique award_id)
    root = Element("awards")
    for t in titles:
        a = SubElement(root, "award")
        SubElement(a, "award_id").text = str(uuid.uuid4())
        SubElement(a, "title").text = dirty_case(t)
        SubElement(a, "category").text = random.choice(
            ["Best Picture", "best director", " Best Actor ", "BEST SCREENPLAY"])
        SubElement(a, "year").text = random.choice([str(random.randint(2018, 2024)), ""])
        SubElement(a, "result").text = random.choice(["won", "WON", "nominated", " Nominated "])
    _put(f"{P}awards.xml", minidom.parseString(tostring(root)).toprettyxml(indent="  "))
    log.info("File raw (CSV/JSON/XML) written to MinIO under %s", P)


def _to_bronze(df, dataset):
    pg = get_pg()
    tbl = TABLES[dataset]["bronze"]
    df = df.astype(object).where(lambda x: x.notna(), None)
    df.to_sql(tbl, pg.get_sqlalchemy_engine(), schema=BRONZE_SCHEMA,
              if_exists="replace", index=False, method="multi", chunksize=1000)
    audit(pg, dataset, "bronze", tbl, len(df))
    log.info("bronze %s.%s: %d rows", BRONZE_SCHEMA, tbl, len(df))


def load_file_bronze(**_):
    s3 = get_s3()
    # CSV -> scores
    rows = list(csv.DictReader(io.StringIO(s3.read_key(f"{P}external_scores.csv", bucket_name=BUCKET_NAME))))
    _to_bronze(pd.DataFrame(rows), "scores")
    # JSON -> watchlist
    events = json.loads(s3.read_key(f"{P}watchlist_events.json", bucket_name=BUCKET_NAME))["events"]
    _to_bronze(pd.DataFrame(events), "watchlist")
    # XML -> awards
    root = ET.fromstring(s3.read_key(f"{P}awards.xml", bucket_name=BUCKET_NAME))
    awards = [{"award_id": a.findtext("award_id"), "title": a.findtext("title"),
               "category": a.findtext("category"), "year": a.findtext("year"),
               "result": a.findtext("result")} for a in root.findall("award")]
    _to_bronze(pd.DataFrame(awards), "awards")
