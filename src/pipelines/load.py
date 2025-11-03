from __future__ import annotations
from pathlib import Path
import pandas as pd
from src.db import get_engine, apply_schema
from src.config import get_database_url

FILES = [
    "movies.csv",
    "genres.csv",
    "movie_genres.csv",
    "users.csv",
    "ratings.csv",
    "omdb_details.csv",
]

def _paths(base: Path):
    return {name: base / name for name in FILES}

def _validate_files(paths: dict[str, Path]):
    missing = [p for p in paths.values() if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing processed files: {', '.join(map(str, missing))}")

def _read_processed(processed_dir: Path):
    paths = _paths(processed_dir)
    _validate_files(paths)

    movies = pd.read_csv(paths["movies.csv"]).astype({"movie_id": "int64"})
    genres = pd.read_csv(paths["genres.csv"]).astype({"genre": "string"})
    movie_genres = pd.read_csv(paths["movie_genres.csv"]).astype({"movie_id": "int64", "genre": "string"})
    users = pd.read_csv(paths["users.csv"]).astype({"user_id": "int64"})
    ratings = pd.read_csv(paths["ratings.csv"]).astype({
        "user_id": "int64",
        "movie_id": "int64",
        "rating": "float64",
        "rating_timestamp": "int64",
    })
    omdb = pd.read_csv(paths["omdb_details.csv"]).astype({
        "movie_id": "int64",
        "imdb_id": "string",
        "director": "string",
        "plot": "string",
        "box_office": "Int64",
        "released_date": "string",
        "runtime_minutes": "Int64",
        "language": "string",
        "country": "string",
    })
    return movies, genres, movie_genres, users, ratings, omdb


def load(processed_dir: Path):
    print("Starting load step...")
    db_url = get_database_url() #Establish database connection
    print(f"Using DATABASE_URL={db_url}")

    schema_path = Path(__file__).resolve().parent.parent.parent / "schema.sql" #Get schema path
    apply_schema(schema_path)

    movies, genres, movie_genres, users, ratings, omdb = _read_processed(processed_dir)
    engine = get_engine()

    with engine.begin() as conn:
        conn.exec_driver_sql("PRAGMA foreign_keys = ON;")

        # Clear all target tables (FK-safe order)
        for tbl in ("ratings", "movie_genres", "omdb_details", "users", "genres", "movies"):
            conn.exec_driver_sql(f"DELETE FROM {tbl};")

        # Insert base tables first
        movies.to_sql("movies", con=conn, if_exists="append", index=False)
        genres.to_sql("genres", con=conn, if_exists="append", index=False)
        users.to_sql("users", con=conn, if_exists="append", index=False)

        # Bridge + fact tables
        movie_genres.to_sql("movie_genres", con=conn, if_exists="append", index=False)
        ratings.to_sql("ratings", con=conn, if_exists="append", index=False)

        # OMDb metadata (if present)
        if not omdb.empty:
            omdb.to_sql("omdb_details", con=conn, if_exists="append", index=False)


    print(" Load complete. Rows inserted:")
    print({
        "movies": len(movies),
        "genres": len(genres),
        "movie_genres": len(movie_genres),
        "users": len(users),
        "ratings": len(ratings),
        "omdb_details": len(omdb),
    })

def run():
    root = Path(__file__).resolve().parents[2]
    processed = root / "data" / "processed"
    load(processed)

if __name__ == "__main__":
    run()
