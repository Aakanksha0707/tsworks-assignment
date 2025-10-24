
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
    #download the zip file, skip if already present 
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        print("Dataset ZIP already present, skipping download.")
        return
    print(f"Downloading MovieLens dataset from {MOVIELENS_URL} ...")

    # process the downloaded zip file
    with requests.get(MOVIELENS_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    print("Download complete.")

def _extract_zip(zip_path: Path, out_dir: Path):

    # extract the zip file, and write the contents to data/raw folder (output_dir)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
    print(f"Extracted to {out_dir}")

def _ensure_data_present(raw_dir: Path):

    #ensure that all the data files like movies.csv, ratings.csv and links.csv are present. 
    ml_dir = raw_dir / "ml-latest-small"
    movies = ml_dir / "movies.csv"
    ratings = ml_dir / "ratings.csv"
    links = ml_dir / "links.csv"

    # If the above mentioned files are not present after extracting, throw an error
    if not (movies.exists() and ratings.exists() and links.exists()):
        raise FileNotFoundError("Expected movies.csv, ratings.csv, and links.csv under data/raw/ml-latest-small/")
    return movies, ratings, links


# That file maps each movieId (MovieLens internal ID) to an external IMDb ID and TMDB ID.
# The IMDb IDs are numeric only (e.g., 114709), The OMDb API expects them in the format tt0114709
def _imdb_tt(imdb_numeric) -> str | None:
    if pd.isna(imdb_numeric):
        return None
    try:
        return "tt" + str(int(imdb_numeric)).zfill(7)
    except Exception:
        return None

#call the omdb api with api key, max retries set to 3 and backoff interval set to 0.5 between each retry
def _fetch_omdb_raw(imdb_id: str, api_key: str, max_retries: int = 3, backoff: float = 0.5):
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

    # make a directory at root/data/raw to store the extracted data files
    root = Path(__file__).resolve().parents[2]
    raw_dir = root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # define the zip file nameand the ml directory name under which the zip files will be present after extraction
    zip_path = raw_dir / "ml-latest-small.zip"
    ml_dir = raw_dir / "ml-latest-small"

    # download the zip file
    _download_zip(zip_path)

    # check if the directory is not present, then extract the zip which will make the directory
    if not ml_dir.exists():
        _extract_zip(zip_path, raw_dir)

    # ensure the files are present in the directory
    movies_csv, ratings_csv, links_csv = _ensure_data_present(raw_dir)
    print("movies.csv and ratings.csv available.")

    # next step is to call the OMDB api to fetch the movie details from the omdb API

    #fetch the API key from .env file to call the OMDB API. If not present, throw an error
    load_env()
    api_key = os.getenv("OMDB_API_KEY")
    if not api_key:
        raise RuntimeError("OMDB_API_KEY not set in .env")

    # limit the number of API calls accoridng to the API usage rule (default is 100). 
    limit = int(os.getenv("OMDB_LIMIT", "100"))

    # read the links.csv file to fetch the OMDB id for each movie
    links_df = pd.read_csv(links_csv)

    # This data set will contain the details of movies fetched from the OMDB API for each movie
    rows = []
    count = 0

    # iterate through the OMDB movie ids from the links.csv file and call the OMDB API for each movie until we hit the limit
    for _, r in links_df.iterrows():

        # fetch the imdb ids by calling the helper function.
        imdb_id = _imdb_tt(r.get("imdbId"))
        if not imdb_id:
            continue

        # call the OMDB API with the id and your API key
        payload = _fetch_omdb_raw(imdb_id, api_key)
        if payload is None:
            continue

        # append a movie id column in the payload which will link the OMDB API response with the movie Id
        payload["_movieId"] = int(r["movieId"]) if not pd.isna(r.get("movieId")) else None

        # append the apyload to the new data set. 
        rows.append(payload)

        # increment the count
        count += 1

        # if count exceeds the limit, stop calling the OMDB API
        if count >= limit:
            break

    # if data is present in the OMDB dataset, dump it inside OMDB csv file
    if rows:
        out_csv = ml_dir / "omdb_raw.csv"
        pd.DataFrame(rows).to_csv(out_csv, index=False)
        print(f"Wrote raw OMDb responses to {out_csv}")
    else:
        print("No OMDb responses fetched; omdb_raw.csv not written.")

if __name__ == "__main__":
    run()
