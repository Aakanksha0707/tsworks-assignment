# Data Engineering ETL Pipeline — MovieLens + OMDb

This project implements a **data engineering pipeline** that extracts movie and rating data from the [MovieLens dataset](https://grouplens.org/datasets/movielens/) and enriches it with metadata from the [OMDb API](https://www.omdbapi.com/).  
The pipeline follows a classic **ETL** (Extract → Transform → Load) architecture using Python and SQLite.

---

## **Project Overview**

The ETL process performs the following:

1. **Extract**  
   - Downloads and extracts the MovieLens dataset (small version).  
   - Reads `movies.csv`, `ratings.csv`, and `links.csv`.  
   - Calls the **OMDb API** using IMDb IDs to fetch movie metadata (e.g., director, box office, runtime).  
   - Writes all raw CSVs to the `data/raw/` directory.

2. **Transform**  
   - Cleans and normalizes raw data.  
   - Splits genres, extracts release years, standardizes timestamps, and parses OMDb fields (box office, runtime, released date).  
   - Writes clean, ready-to-load CSVs to `data/processed/`.

3. **Load**  
   - Applies the database schema (idempotent DDL).  
   - Loads all processed CSVs into a local SQLite database `data/movies.db`.  
   - Creates indexes for performance.

---

## **Project Structure**


<pre> ``` 
assignment/
├── data/
│ ├── raw/ 
│ │ ├── ml-latest-small/ # Original MovieLens data and the omdb enriched data
│ ├── processed/ # Processed data files
│ │ ├── movies.csv
│ │ ├── genres.csv
│ │ ├── movie_genres.csv
│ │ ├── users.csv
│ │ ├── ratings.csv
│ │ ├── omdb_details.csv
│ └── movies.db # SQLite database
│
├── sql/
│ └── schema.sql # Database DDL (idempotent)
│
├── src/
│ ├── config.py # Loads .env and DB connection
│ ├── db.py # Engine + schema apply logic
│ ├── run_etl.py # ETL orchestrator (extract→transform→load)
│ └── pipelines/
│  ├── extract_movielens.py # Extract step (MovieLens + OMDb)
│  ├── transform_from_raw.py # Transform step (normalize data)
│  └── load_from_processed.py # Load step (DB insert)
│
├── .env # Contains OMDb API key, data base details
└── README.md # Documentation 
```</pre>

---

##  **Setup Instructions**

### Install python on your machine
https://www.python.org/downloads/


### Clone the repository
<pre>git clone https://github.com/Aakanksha0707/tsworks-assignment.git
cd asignment
</pre>


### Create and activate a virtual environment
<pre>python3 -m venv .venv
source .venv/bin/activate
</pre>

### Install dependencies
<pre>pip install -r requirements.txt
</pre>

### Set up the .env file
In the project root, make sure to change the values in .env file 
Change the OMDB API key to your API Key

### Run the pipeline
<pre>python3 etl.py
</pre>


### Run individual steps sequentially in the pipeline if needed (Optional)

Extract : 
<pre>python3 -m src.pipelines.extract
</pre>

Transform : 
<pre>python3 -m src.pipelines.transform
</pre>

Load : 
<pre>python3 -m src.pipelines.load
</pre>

---

##  **Database Schema**

| Table          | Description                                                  |
| -------------- | ------------------------------------------------------------ |
| `movies`       | Movie information (ID, title, year)                          |
| `genres`       | Unique list of genres                                        |
| `movie_genres` | Many-to-many link between movies and genres                  |
| `users`        | List of unique user IDs                                      |
| `ratings`      | User ratings for movies                                      |
| `omdb_details` | Metadata from OMDb API (director, box office, runtime, etc.) |

Indexes are created on ratings(movie_id), ratings(user_id), and movie_genres(genre).


## Design Choices & Assumptions

### Design Choices
- **Three-step ETL (Extract → Transform → Load):**  
  Each stage is modular and single-responsibility. Extraction handles data acquisition, transformation handles all cleaning and normalization, and loading focuses only on schema application and inserts.

- **Two data layers (`raw/` and `processed/`):**  
  Keeps raw data untouched and stores clean, schema-ready CSVs separately.

- **SQLite for simplicity:**  
  Lightweight, file-based database ideal for local ETL testing.

- **Idempotent schema:**  
  DDL is re-applied safely on every load, allowing full refresh runs without setup steps.

- **Environment-driven config:**  
  `.env` file controls API keys, database URL, and limits.

- **OMDb integrated in Extract step:**  
  Raw API responses are stored in `omdb_raw.csv` for reproducibility and offline testing.

---

### Assumptions
- **Dataset:** Uses MovieLens “latest small” dataset with standard structure (`movies.csv`, `ratings.csv`, `links.csv`).
- **Ratings:** Range 0–5; timestamps parsed.
- **Movies:** Year parsed from title suffix `(YYYY)` when present; otherwise `NULL`.
- **Genres:** Split on `|`; entries like `(no genres listed)` are ignored.
- **OMDb API:** Limited calls (`OMDB_LIMIT`); raw numeric IMDb IDs converted to `tt` format (e.g., `tt0114709`).
- **OMDb fields:** Basic parsing for `BoxOffice`, `Runtime`, `Released`; text fields stored as-is.
- **Foreign keys:** Enforced via `PRAGMA foreign_keys=ON` for relational consistency.
- **Environment:** `.env` must include `OMDB_API_KEY`; defaults set for limit and database URL.


## Challenges & How I Overcame Them

- **Path issues:** Fixed import and file path errors using `Path(__file__).resolve().parents[2]` for dynamic project root detection.  
- **Environment setup:** Standardized config loading via `.env` and `load_env()` to manage API keys and DB URLs.  
- **API rate limits:** Handled OMDb free-tier constraints using retry logic, backoff, and a configurable `OMDB_LIMIT`.  
- **Data inconsistencies:** Normalized varied OMDb fields (`BoxOffice`, `Runtime`, `Released`) with type-safe parsing.  
- **Idempotent runs:** Made schema creation and loads repeatable by applying DDL safely and using full-refresh inserts.  
- **Integrity checks:** Ensured data consistency by enabling `PRAGMA foreign_keys=ON` during all database operations.


##  **Author**
Aakanksha Pandey
pandeyaakanksha16@gmail.com