# ============================================================
# CineVerse | gold.py
# Silver -> Gold aggregations, done with SQL inside Postgres.
# Generic load_to_gold dispatches a SQL builder per dataset,
# plus build_movie_360 which joins everything.
# ============================================================
import logging

from cineverse.common import get_pg, audit
from cineverse.config import SILVER_SCHEMA, GOLD_SCHEMA

log = logging.getLogger(__name__)


def _gold_sql(dataset, s, st, g, gt):
    """Return the DROP+CREATE-AS-SELECT SQL for one dataset's gold table."""
    if dataset == "movies":
        body = f"""SELECT COALESCE(genre,'Unknown') AS genre, type,
                          COUNT(*) AS titles, ROUND(AVG(rating)::numeric,2) AS avg_rating,
                          ROUND(AVG(runtime_min)::numeric,0) AS avg_runtime_min
                   FROM {s}.{st} GROUP BY COALESCE(genre,'Unknown'), type"""
    elif dataset == "ratings":
        body = f"""SELECT movie_key, COUNT(*) AS rating_count,
                          ROUND(AVG(score)::numeric,2) AS avg_score,
                          COUNT(DISTINCT user_id) AS unique_raters
                   FROM {s}.{st} WHERE score IS NOT NULL GROUP BY movie_key"""
    elif dataset == "reviews":
        body = f"""SELECT movie_key, COUNT(*) AS review_count,
                          COUNT(*) FILTER (WHERE sentiment='positive') AS positive,
                          COUNT(*) FILTER (WHERE sentiment='negative') AS negative
                   FROM {s}.{st} WHERE movie_key IS NOT NULL GROUP BY movie_key"""
    elif dataset == "scores":
        body = f"""SELECT movie_key, MAX(imdb_score) AS imdb_score, MAX(rt_score) AS rt_score
                   FROM {s}.{st} GROUP BY movie_key"""
    elif dataset == "watchlist":
        body = f"""SELECT movie_key,
                          COUNT(*) FILTER (WHERE action='add') AS adds,
                          COUNT(*) FILTER (WHERE action='remove') AS removes
                   FROM {s}.{st} WHERE movie_key IS NOT NULL GROUP BY movie_key"""
    elif dataset == "awards":
        body = f"""SELECT movie_key,
                          COUNT(*) FILTER (WHERE result='won') AS wins,
                          COUNT(*) FILTER (WHERE result='nominated') AS nominations
                   FROM {s}.{st} WHERE movie_key IS NOT NULL GROUP BY movie_key"""
    elif dataset == "users":
        body = f"""SELECT COALESCE(country,'Unknown') AS country, COUNT(*) AS users,
                          SUM(CASE WHEN is_premium THEN 1 ELSE 0 END) AS premium_users
                   FROM {s}.{st} GROUP BY COALESCE(country,'Unknown')"""
    else:
        raise ValueError(f"no gold builder for dataset {dataset}")
    return f"DROP TABLE IF EXISTS {g}.{gt}; CREATE TABLE {g}.{gt} AS {body};"


def load_to_gold(dataset, silver_schema, silver_table, gold_schema, gold_table, **_):
    pg = get_pg()
    pg.run(_gold_sql(dataset, silver_schema, silver_table, gold_schema, gold_table))
    n = pg.get_first(f"SELECT COUNT(*) FROM {gold_schema}.{gold_table}")[0]
    audit(pg, dataset, "gold", gold_table, n)
    log.info("Gold %s.%s: %d rows", gold_schema, gold_table, n)


def build_movie_360(**_):
    """Cross-source Gold mart: one row per movie joining all sources."""
    pg = get_pg()
    s, g = SILVER_SCHEMA, GOLD_SCHEMA
    pg.run(f"""
        DROP TABLE IF EXISTS {g}.movie_360;
        CREATE TABLE {g}.movie_360 AS
        SELECT m.movie_key, m.title, m.type, m.genre, m.release_date, m.runtime_min,
               m.rating AS catalog_rating,
               fr.avg_score AS user_avg_score, fr.rating_count,
               ss.imdb_score, ss.rt_score,
               tp.adds AS watchlist_adds,
               aw.wins AS award_wins,
               rv.review_count
        FROM {s}.movies m
        LEFT JOIN {g}.fact_ratings     fr ON fr.movie_key = m.movie_key
        LEFT JOIN {g}.score_summary    ss ON ss.movie_key = m.movie_key
        LEFT JOIN {g}.title_popularity tp ON tp.movie_key = m.movie_key
        LEFT JOIN {g}.awards_won       aw ON aw.movie_key = m.movie_key
        LEFT JOIN {g}.review_summary   rv ON rv.movie_key = m.movie_key;

        DROP TABLE IF EXISTS {g}.trending_titles;
        CREATE TABLE {g}.trending_titles AS
        SELECT movie_key, title, genre, watchlist_adds, user_avg_score, award_wins
        FROM {g}.movie_360
        ORDER BY COALESCE(watchlist_adds,0) DESC, COALESCE(user_avg_score,0) DESC
        LIMIT 20;
    """)
    n = pg.get_first(f"SELECT COUNT(*) FROM {g}.movie_360")[0]
    audit(pg, "marts", "gold", "movie_360", n)
    log.info("Gold %s.movie_360: %d rows", g, n)


def record_run_metrics(**_):
    """Append one timestamped snapshot of the catalog totals each run, so the
    pipeline's run-by-run GROWTH can be charted over time (Gold layer)."""
    pg = get_pg()
    g, s = GOLD_SCHEMA, SILVER_SCHEMA
    # create once (append-only history table -> NOT dropped)
    pg.run(f"""
        CREATE TABLE IF NOT EXISTS {g}.run_metrics (
            run_time      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_movies  INTEGER,
            total_users   INTEGER,
            total_ratings INTEGER,
            total_reviews INTEGER
        );
    """)

    def cnt(schema, table):
        try:
            return pg.get_first(f"SELECT COUNT(*) FROM {schema}.{table}")[0]
        except Exception:
            return 0

    movies = cnt(s, "movies")
    users = cnt(s, "users")
    ratings = cnt(s, "ratings")
    reviews = cnt(s, "reviews")
    pg.run(f"""INSERT INTO {g}.run_metrics
               (total_movies, total_users, total_ratings, total_reviews)
               VALUES (%s, %s, %s, %s)""",
           parameters=(int(movies), int(users), int(ratings), int(reviews)))
    audit(pg, "marts", "gold", "run_metrics", movies)
    log.info("run_metrics snapshot: movies=%d users=%d ratings=%d reviews=%d",
             movies, users, ratings, reviews)