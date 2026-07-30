# ============================================================
# CineVerse | cleaning.py
# Per-dataset PySpark cleaning (Bronze DataFrame -> Silver DataFrame).
# Each function takes a Spark DataFrame read from the bronze table and
# returns the cleaned Silver DataFrame.
# ============================================================
from pyspark.sql import functions as F, Window


def _to_date(col):
    return F.date_format(F.coalesce(
        F.to_date(col, "yyyy-MM-dd"), F.to_date(col, "dd/MM/yyyy"),
        F.to_date(col, "MM-dd-yyyy"), F.to_date(col, "yyyy/MM/dd"),
        F.to_date(col, "MMMM d, yyyy"), F.to_date(col, "MMM d yyyy")), "yyyy-MM-dd")


def _mkey(col):
    t = F.initcap(F.trim(col))
    return F.when(t.isNotNull() & (t != ""), F.concat_ws("|", t, F.lit("movie")))


def clean_movies(df):
    return (df
        .withColumn("title", F.initcap(F.trim("title")))
        .withColumn("type", F.lit("movie"))
        .withColumn("genre", F.initcap(F.trim("genre")))
        .withColumn("language", F.lower(F.trim("language")))
        .withColumn("release_date", _to_date("release_date"))
        .withColumn("runtime_min", F.regexp_replace("runtime", "[^0-9]", "").cast("int"))
        .withColumn("runtime_min", F.when((F.col("runtime_min") >= 1) & (F.col("runtime_min") <= 600), F.col("runtime_min")))
        .withColumn("rating", F.regexp_replace(F.split("rating", "/").getItem(0), "[^0-9.]", "").cast("double"))
        .withColumn("rating", F.when((F.col("rating") >= 0) & (F.col("rating") <= 10), F.col("rating")))
        .withColumn("votes", F.regexp_replace("votes", "[^0-9]", "").cast("int"))
        .withColumn("director", F.trim("director"))
        .filter(F.col("title").isNotNull() & (F.col("title") != ""))
        .withColumn("movie_key", F.concat_ws("|", F.col("title"), F.col("type")))
        .dropDuplicates(["movie_key"])
        .select("movie_key", "title", "type", "genre", "language", "release_date",
                "runtime_min", "rating", "votes", "director"))


def clean_users(df):
    df = (df
        .withColumn("user_id", F.col("user_id").cast("int"))
        .withColumn("username", F.trim("username"))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("country", F.initcap(F.trim("country")))
        .withColumn("signup_date", _to_date("signup_date"))
        .withColumn("is_premium",
            F.when(F.lower(F.trim("is_premium")).isin("true", "1", "yes", "y", "t"), F.lit(True))
             .when(F.lower(F.trim("is_premium")).isin("false", "0", "no", "n", "f"), F.lit(False))))
    w = Window.partitionBy("email").orderBy("user_id")
    return (df.withColumn("_rn", F.when(F.col("email").isNull(), F.lit(1)).otherwise(F.row_number().over(w)))
              .filter(F.col("_rn") == 1).drop("_rn")
              .select("user_id", "username", "email", "country", "signup_date", "is_premium"))


def clean_ratings(df):
    return (df
        .withColumn("rating_id", F.col("rating_id").cast("int"))
        .withColumn("user_id", F.col("user_id").cast("int"))
        .withColumn("movie_key", _mkey(F.col("movie_title")))
        .withColumn("score", F.regexp_replace("score", "[^0-9.]", "").cast("double"))
        .withColumn("score", F.when((F.col("score") >= 0) & (F.col("score") <= 10), F.col("score")))
        .withColumn("watched_date", _to_date("watched_date"))
        .dropDuplicates(["rating_id"])
        .select("rating_id", "user_id", "movie_key", "score", "watched_date"))


def clean_reviews(df):
    return (df
        .withColumn("review_id", F.col("review_id").cast("int"))
        .withColumn("user_id", F.col("user_id").cast("int"))
        .withColumn("movie_key", _mkey(F.col("movie_title")))
        .withColumn("sentiment", F.lower(F.trim("sentiment")))
        .withColumn("body", F.trim("body"))
        .withColumn("created_date", _to_date("created_date"))
        .dropDuplicates(["review_id"])
        .select("review_id", "user_id", "movie_key", "sentiment", "body", "created_date"))


def clean_scores(df):
    src = F.lower(F.trim("source"))
    return (df
        .withColumn("movie_key", _mkey(F.col("title")))
        .withColumn("imdb_score", F.regexp_replace("imdb_score", "[^0-9.]", "").cast("double"))
        .withColumn("imdb_score", F.when((F.col("imdb_score") >= 0) & (F.col("imdb_score") <= 10), F.col("imdb_score")))
        .withColumn("rt_score", F.regexp_replace("rt_score", "[^0-9]", "").cast("int"))
        .withColumn("rt_score", F.when((F.col("rt_score") >= 0) & (F.col("rt_score") <= 100), F.col("rt_score")))
        .withColumn("source", F.when(src.isin("rt", "rotten_tomatoes"), F.lit("rotten_tomatoes")).otherwise(F.lit("imdb")))
        .withColumn("collected_at", _to_date("collected_at"))
        .filter(F.col("movie_key").isNotNull())
        .dropDuplicates(["movie_key", "source"])
        .select("movie_key", "imdb_score", "rt_score", "source", "collected_at"))


def clean_watchlist(df):
    return (df
        .withColumn("username", F.trim("user"))
        .withColumn("movie_key", _mkey(F.col("title")))
        .withColumn("action", F.lower(F.trim("action")))
        .withColumn("event_date", _to_date("event_time"))
        .withColumn("device", F.lower(F.trim("device")))
        .dropDuplicates(["event_id"])
        .select("username", "movie_key", "action", "event_date", "device"))


def clean_awards(df):
    return (df
        .withColumn("movie_key", _mkey(F.col("title")))
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("year", F.regexp_replace("year", "[^0-9]", "").cast("int"))
        .withColumn("result", F.lower(F.trim("result")))
        .dropDuplicates(["award_id"])
        .select("movie_key", "category", "year", "result"))


CLEANERS = {
    "movies": clean_movies,
    "users": clean_users,
    "ratings": clean_ratings,
    "reviews": clean_reviews,
    "scores": clean_scores,
    "watchlist": clean_watchlist,
    "awards": clean_awards,
}
