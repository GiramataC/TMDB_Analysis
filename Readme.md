# TMDB Movie Analysis Pipeline

A data pipeline that fetches, cleans, analyses, and visualizes movie data from The Movie Database (TMDB) API.

---

## Project Structure

 data/
│   ├── raw/
│   │   ├── movies.parquet
│   │   └── movies_output.csv
│   └── processed/
│       └── movies_cleaned.csv
├── logs/
│   └── movie_pipeline.log
├── Data_retrieval.py
├── Data_cleaning.py
├── Analysis.py
├── visualisation.py
├── main.py
├── config.py
├── TMDB.ipynb
├── .env
└── README.md

### Set up your API key

You can get a free API key from [https://www.themoviedb.org/settings/api](https://www.themoviedb.org/settings/api)

### Configure movie IDs
Edit `config.py` to set the list of TMDB movie IDs you want to fetch:
```python
MOVIE_IDS = [299536, 19995, 140607, ...]
```

---

## How to Run

### Run the full pipeline
```bash
python main.py
```
This will:
1. Fetch movie data from the TMDB API
2. Clean and preprocess the data
3. Engineer KPI features
4. Print analysis results to the terminal
5. Display visualizations

### Run the notebook for interactive exploration
```bash
jupyter notebook TMDB.ipynb
```

---

## Pipeline Overview

| File | Purpose |
|---|---|
| `Data_retrieval.py` | Fetches movie details and credits from TMDB API with retry logic |
| `Data_cleaning.py` | Cleans raw data, flattens nested fields, handles missing values |
| `Analysis.py` | KPI engineering, ranking, filtering, and aggregation queries |
| `visualisation.py` | Matplotlib charts for revenue, ROI, popularity, and franchise analysis |
| `main.py` | Orchestrates the full pipeline end to end |
| `config.py` | Stores movie IDs and pipeline configuration constants |
| `TMDB.ipynb` | Interactive notebook for exploring results |

---

## Features

- Fetches movie metadata and credits in a single API call using `append_to_response`
- Retry logic with exponential backoff for failed API requests
- Handles nested JSON fields like genres, cast, and production companies
- Computes KPIs: profit, ROI, budget and revenue in millions
- Flexible ranking and filtering system supporting numeric and string conditions
- Franchise vs standalone performance comparison
- Top directors and franchises by revenue
- Visualizations: revenue vs budget, ROI by genre, popularity vs rating, yearly trends

---

## Logs

Pipeline logs are saved to `logs/movie_pipeline.log` and also printed to the terminal during each run.

---

## Notes

- The `.env` file is excluded from version control via `.gitignore` — never commit your API key
- Raw data is saved as both `.parquet` and `.csv` for flexibility
- Always run `main.py` first before opening the notebook to ensure data is up to date