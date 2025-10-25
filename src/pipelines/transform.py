from __future__ import annotations
from pathlib import Path
import re
import pandas as pd

# This file is for cleaning the data and transforming the columns into structured data 
# bothe from movie lens dataset and the enriched data received from OMDB API


# Infer the year from the movie title
def _infer_year_from_title(title: str) -> int | None:
    # if the movie name is not a string data type return none
    if not isinstance(title, str):
        return None
    
    # A RegEx searching for opening and close brackets in the movie name with 4 digits in between which is the year
    m = re.search(r"\((\d{4})\)$", title.strip())
    if m:
        # if the year is found check it is within 1870 and 2100 else return None
        try:
            y = int(m.group(1))
            if 1870 <= y <= 2100:
                return y
        except ValueError:
            return None
    return None

# Parse the box office value
def _parse_box_office(value):
    # if the box office value is not a string or is NA, return None
    if not isinstance(value, str) or value == "N/A":
        return None
    
    # Conver the box office value to an a digit and then parse it as an integer
    digits = re.sub(r"[^0-9]", "", value)
    return int(digits) if digits else None

# Parse the runtime value
def _parse_runtime(value):
    # if the runtime value is not a string instance or NA return None
    if not isinstance(value, str) or value == "N/A":
        return None
    # Search for digits in the runtime and parse it into an integer or else return None
    match = re.search(r"(\d+)", value)
    return int(match.group(1)) if match else None

# Parse the released value
def _parse_released(value):
    # if the released value is not a string or NA return NA
    if not isinstance(value, str) or value == "N/A":
        return None
    # Try to convert the released value to a date value
    try:
        return pd.to_datetime(value).date().isoformat()
    except Exception:
        return None


# Actual transform function which will transform the data 
def transform(raw_dir: Path, out_dir: Path) -> None:

    # set the output dir as data/processed
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
