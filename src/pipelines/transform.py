from __future__ import annotations
from pathlib import Path
import re
import pandas as pd

def _infer_year_from_title(title: str) -> int | None:
    if not isinstance(title, str):
        return None
    m = re.search(r"\((\d{4})\)$", title.strip())
    if m:
        try:
            y = int(m.group(1))
            if 1870 <= y <= 2100:
                return y
        except ValueError:
            return None
    return None

def _parse_box_office(value):
    if not isinstance(value, str) or value == "N/A":
        return None
    digits = re.sub(r"[^0-9]", "", value)
    return int(digits) if digits else None

def _parse_runtime(value):
    if not isinstance(value, str) or value == "N/A":
        return None
    match = re.search(r"(\d+)", value)
    return int(match.group(1)) if match else None

def _parse_released(value):
    if not isinstance(value, str) or value == "N/A":
        return None
    try:
        return pd.to_datetime(value).date().isoformat()
    except Exception:
        return None


def transform(raw_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- MovieLens base CSVs ----
    movies_path = raw_dir / "ml-latest-small" / "movies.csv"
    ratings_path = raw_dir / "ml-latest-small" / "ratings.csv"
    if not movies_path.exists() or not ratings_path.exists():
        raise FileNotFoundError("Expected movies.csv and ratings.csv under data/raw/ml-latest-small/")

    movies_df = pd.read_csv(movies_path)
    ratings_df = pd.read_csv(ratings_path)

    # ---- Movies normalization ----
    movies_df["year"] = movies_df["title"].apply(_infer_year_from_title).astype("Int64")
    movies_norm = movies_df[["movieId", "title", "year"]].rename(columns={"movieId": "movie_id"})

    # ---- Genres normalization ----
    exploded = (
        movies_df.assign(genres=movies_df["genres"].fillna(""))
        .assign(genres=lambda d: d["genres"].str.split("|"))
        .explode("genres")
    )
    exploded["genres"] = exploded["genres"].fillna("").str.strip()
    exploded = exploded[exploded["genres"] != ""]
    exploded = exploded[exploded["genres"].str.lower() != "(no genres listed)"]

    movie_genres = (
        exploded[["movieId", "genres"]]
        .rename(columns={"movieId": "movie_id", "genres": "genre"})
        .drop_duplicates()
        .sort_values(["movie_id", "genre"])
        .reset_index(drop=True)
    )
    genres = movie_genres[["genre"]].drop_duplicates().sort_values("genre").reset_index(drop=True)

    # ---- Users from ratings ----
    users = (
        ratings_df[["userId"]]
        .drop_duplicates()
        .rename(columns={"userId": "user_id"})
        .sort_values("user_id")
        .reset_index(drop=True)
    )

    # ---- Ratings cleanup ----
    ratings_clean = (
        ratings_df.rename(columns={"userId": "user_id", "movieId": "movie_id"})[
            ["user_id", "movie_id", "rating", "timestamp"]
        ]
        .rename(columns={"timestamp": "rating_timestamp"})
    )
    ratings_clean = ratings_clean.astype(
        {"user_id": "int64", "movie_id": "int64", "rating": "float64", "rating_timestamp": "int64"}
    )

    # ---- OMDb normalization ----
    omdb_raw_path = raw_dir / "ml-latest-small" / "omdb_raw.csv"
    if omdb_raw_path.exists():
        omdb_df = pd.read_csv(omdb_raw_path, low_memory=False)
        if "_movieId" not in omdb_df.columns:
            print("OMDb file missing _movieId column — skipping OMDb transform.")
            omdb_clean = pd.DataFrame()
        else:
            omdb_clean = pd.DataFrame({
                "movie_id": omdb_df["_movieId"],
                "imdb_id": omdb_df.get("imdbID"),
                "director": omdb_df.get("Director"),
                "plot": omdb_df.get("Plot"),
                "box_office": omdb_df.get("BoxOffice").apply(_parse_box_office),
                "released_date": omdb_df.get("Released").apply(_parse_released),
                "runtime_minutes": omdb_df.get("Runtime").apply(_parse_runtime),
                "language": omdb_df.get("Language"),
                "country": omdb_df.get("Country"),
            }).dropna(subset=["movie_id"])
            omdb_clean["movie_id"] = omdb_clean["movie_id"].astype("int64")
    else:
        print("No omdb_raw.csv found — skipping OMDb transformation.")
        omdb_clean = pd.DataFrame()

    # ---- Write outputs ----
    movies_norm.to_csv(out_dir / "movies.csv", index=False)
    genres.to_csv(out_dir / "genres.csv", index=False)
    movie_genres.to_csv(out_dir / "movie_genres.csv", index=False)
    users.to_csv(out_dir / "users.csv", index=False)
    ratings_clean.to_csv(out_dir / "ratings.csv", index=False)
    if not omdb_clean.empty:
        omdb_clean.to_csv(out_dir / "omdb_details.csv", index=False)

    print("Transformation complete. Processed files written to:", out_dir)


def run():
    root = Path(__file__).resolve().parents[2]
    raw_dir = root / "data" / "raw"
    out_dir = root / "data" / "processed"
    transform(raw_dir, out_dir)


if __name__ == "__main__":
    run()
