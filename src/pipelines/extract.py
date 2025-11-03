from __future__ import annotations
import os
import time
from pathlib import Path
import zipfile
import requests
import pandas as pd
from src.config import load_env

MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"

def _download_zip(zip_path: Path):
    """Download the MovieLens zip file if not already present."""
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        print("Dataset ZIP already present, skipping download.")
        return
    print(f"Downloading MovieLens dataset from {MOVIELENS_URL} ...")

    # Stream download and save to disk
    with requests.get(MOVIELENS_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    print("Download complete.")

def _extract_zip(zip_path: Path, out_dir: Path): 
    """Extract the downloaded MovieLens zip file into the raw data folder."""
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
    print(f"Extracted to {out_dir}")

def _ensure_data_present(raw_dir: Path):
    """Ensure required MovieLens CSVs (movies, ratings, links) exist."""
    ml_dir = raw_dir / "ml-latest-small"
    movies = ml_dir / "movies.csv"
    ratings = ml_dir / "ratings.csv"
    links = ml_dir / "links.csv"

    if not (movies.exists() and ratings.exists() and links.exists()):
        raise FileNotFoundError("Expected movies.csv, ratings.csv, and links.csv under data/raw/ml-latest-small/")
    return movies, ratings, links

def _imdb_tt(imdb_numeric) -> str | None:
    """Convert IMDb numeric IDs to OMDb API format (e.g. 114709 → tt0114709)."""
    if pd.isna(imdb_numeric):
        return None
    try:
        return "tt" + str(int(imdb_numeric)).zfill(7)
    except Exception:
        return None

def _fetch_omdb_raw(imdb_id: str, api_key: str, max_retries: int = 3, backoff: float = 0.5):
    """Call the OMDb API for a given IMDb ID and return the raw JSON response."""
    params = {"i": imdb_id, "apikey": api_key}
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get("https://www.omdbapi.com/", params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException:
            time.sleep(backoff * attempt)
    return None

def run():
    """Extract MovieLens data and fetch raw OMDb metadata."""

    # Create /data/raw directory
    root = Path(__file__).resolve().parents[2]
    raw_dir = root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Define zip file path and extraction directory
    zip_path = raw_dir / "ml-latest-small.zip"
    ml_dir = raw_dir / "ml-latest-small"

    # Download MovieLens dataset (if missing)
    _download_zip(zip_path)

    # Extract the zip if not already extracted
    if not ml_dir.exists():
        _extract_zip(zip_path, raw_dir)

    # Verify required files exist
    movies_csv, ratings_csv, links_csv = _ensure_data_present(raw_dir)
    print("movies.csv and ratings.csv available.")

    # Load environment variables to access the OMDb API key
    load_env()
    api_key = os.getenv("OMDB_API_KEY")
    if not api_key:
        raise RuntimeError("OMDB_API_KEY not set in .env")

    # Limit OMDb API calls (default: 100)
    limit = int(os.getenv("OMDB_LIMIT", "100"))

    # Read links.csv (maps MovieLens movieId → IMDb ID)
    links_df = pd.read_csv(links_csv)

    rows, count = [], 0

    # Iterate through IMDb IDs and fetch OMDb data
    for _, r in links_df.iterrows():
        imdb_id = _imdb_tt(r.get("imdbId"))
        if not imdb_id:
            continue

        # Fetch OMDb data
        payload = _fetch_omdb_raw(imdb_id, api_key)
        if payload is None:
            continue

        # Add MovieLens movieId to OMDb payload for linkage
        payload["_movieId"] = int(r["movieId"]) if not pd.isna(r.get("movieId")) else None
        rows.append(payload)
        count += 1

        # Stop once the limit is reached
        if count >= limit:
            break

    # Save all OMDb responses to CSV (raw format, untransformed)
    if rows:
        out_csv = ml_dir / "omdb_raw.csv"
        pd.DataFrame(rows).to_csv(out_csv, index=False)
        print(f"Wrote raw OMDb responses to {out_csv}")
    else:
        print("No OMDb responses fetched; omdb_raw.csv not written.")

if __name__ == "__main__":
    run()
