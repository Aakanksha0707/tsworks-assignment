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
<pre>python -m src.run_etl
</pre>


### Run individual steps sequentially in the pipeline if needed (Optional)

Extract : 
<pre>python -m src.pipelines.extract
</pre>

Transform : 
<pre>python -m src.pipelines.transform
</pre>

Load : 
<pre>python -m src.pipelines.load
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


##  **Author**
Aakanksha Pandey
pandeyaakanksha16@gmail.com