# TMDB Movie Analysis Pipeline

A production-grade data pipeline that fetches, cleans, analyzes, and visualizes movie data from The Movie Database (TMDB) API. Designed for reliability with strict rate limiting, async concurrency, comprehensive validation, and automatic failure tracking.

---

## Project Structure

```
data/
│   ├── raw/
│   │   ├── movies.parquet              # Raw API data (active format)
│   │   └── failed_movies.csv           # Failed movie IDs (audit trail)
│   └── processed/
│       └── movies_cleaned.csv          # Clean, validated movie data
├── logs/
│   └── movie_pipeline.log              # Comprehensive pipeline logs
├── visualizations/                     # Auto-generated charts (timestamped)
│   ├── 01_revenue_vs_budget_*.html
│   ├── 02_roi_by_genre_*.html
│   ├── 03_popularity_vs_rating_*.png
│   ├── 04_yearly_revenue_*.png
│   └── 05_franchise_vs_standalone_*.png
├── Data_retrieval.py
├── Data_cleaning.py
├── Analysis.py
├── visualisation.py
├── validators.py                       # Pydantic schemas for data validation
├── rate_limiter.py                     # Token bucket, circuit breaker, backoff
├── main.py
├── config.py
├── TMDB.ipynb
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

## Setup

### Clone the repository
```bash
git clone <your-repo-url>
cd TMDB_Analysis
```

### Create and activate a virtual environment
```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows
```

### Install dependencies
```bash
pip install -r requirements.txt
```

### Configure API Key
Get a free API key from [TMDB API Settings](https://www.themoviedb.org/settings/api)

Create a `.env` file in the project root:
```bash
API_KEY=your_tmdb_api_key_here
```

**⚠️ IMPORTANT:** Never commit `.env` or `config.py` (both in `.gitignore`)

### Configure Movie IDs
Edit `config.py` to specify which movies to fetch:
```python
MOVIE_IDS = [299536, 19995, 140607, ...]  # TMDB movie IDs
```

---

## Running the Pipeline

### Full pipeline execution
```bash
python main.py
```

This will:
1. Fetch movie data from TMDB API (with strict rate limiting)
2. Clean and validate data (3-stage Pydantic validation)
3. Engineer KPI features (profit, ROI)
4. Run analysis (top movies, top directors)
5. Generate visualizations (saved to `visualizations/` directory)
6. Log all operations to `logs/movie_pipeline.log`

### Notebook-based exploration
```bash
jupyter notebook TMDB.ipynb
```

---

## Pipeline Components

| Module | Purpose |
|--------|---------|
| **Data_retrieval.py** | Async API fetching with rate limiting, circuit breaker, and validation |
| **Data_cleaning.py** | Data preprocessing, type conversion, missing value handling |
| **Analysis.py** | KPI engineering, ranking, filtering, aggregation queries |
| **visualisation.py** | Interactive (HTML) and static (PNG) chart generation |
| **rate_limiter.py** | Token bucket, circuit breaker, exponential backoff with jitter |
| **validators.py** | Pydantic schemas for API response, cleaned data, and KPI validation |
| **config.py** | Centralized configuration (rate limiting, timeouts, session settings) |
| **main.py** | Pipeline orchestration and error handling |

---

## Production Features

### Rate Limiting
- **Token bucket**: 4 req/sec (TMDB limit: 40 req/10s)
- **Circuit breaker**: Stops requests after 5 failures, recovers after 60s
- **Exponential backoff**: 1s → 2s → 4s → 8s → 16s → 32s (with ±10% jitter)
- **Monitoring**: Tracks requests, blocks, and circuit breaks

**Configuration** (in `config.py`):
```python
RATE_LIMITING = {
    "requests_per_second": 4,
    "max_retries": 5,
    "circuit_breaker_threshold": 5,
    "circuit_breaker_timeout": 60
}
```

### Async Concurrency
- **4 concurrent workers** (asyncio.Semaphore)
- **Non-blocking HTTP** with aiohttp 3.9.1
- **Optimized session**: 4 total connections, 10 per-host, 30s timeout
- **Result**: ~4.8 req/s (respects rate limit)

### Operation Timeout
- **Global 600s timeout** (asyncio.timeout)
- Prevents runaway processes and hanging connections
- Configurable: `OPERATION_TIMEOUT = 600` in `config.py`

### Multi-Stage Data Validation
**3 validation stages:**
1. **API Response** → Validates title, budget, revenue, release_date using Pydantic
2. **Cleaned Data** → Checks for duplicates, missing values, empty datasets
3. **KPI Calculation** → Prevents division-by-zero, inf/NaN propagation

**Validation catches:**
- Invalid field format (e.g., bad date)
- Unrealistic values (e.g., budget > $5B)
- Data quality issues (e.g., >50% null columns)
- Mathematical errors (e.g., division by zero)

### Failed Movie Tracking
- Saves failed movie IDs to `data/raw/failed_movies.csv`
- Includes timestamp and error notes for audit trail
- Easy retry: can re-run with failed IDs

### Visualizations
- **2 Interactive HTML charts** (Plotly): Zoom, hover, filter
  - Revenue vs Budget scatter plot
  - ROI distribution by genre (box plot)
- **3 Static PNG charts** (Matplotlib, 300 DPI):
  - Popularity vs Rating
  - Yearly Box Office trends
  - Franchise vs Standalone comparison
- **Timestamped filenames** prevent overwrites (e.g., `01_revenue_vs_budget_20260409_225521.html`)

### Comprehensive Logging
- **Dual output**: Console + file (`logs/movie_pipeline.log`)
- **Structured format**: `[TIMESTAMP] [LEVEL] Message`
- **Tracks**:
  - API key validation
  - Rate limit stats (requests/sec, blocked %, circuit breaks)
  - Data quality checks
  - Validation errors (with full Pydantic error details)
  - Failed movie IDs
  - File save locations

Example log snippet:
```
2026-04-09 22:48:15,356 [INFO] Async Rate Limiting initialized: 4 req/s | Concurrency: 4 workers | Circuit breaker: 5 failures, 60s timeout
2026-04-09 22:48:19,371 [INFO] Fetching finished. Requested: 18 | Successful: 18 | Failed: 0 | Success Rate: 100.0%
2026-04-09 22:48:19,373 [INFO] Rate Limit Stats | Requests: 18 | Rate: 4.82 req/s | Blocked: 0.0% | Circuit breaks: 0 | Elapsed: 3.7s
2026-04-09 22:48:22,203 [INFO] Saved 5 visualizations to visualizations/ directory
```

---

## Configuration Reference

### Rate Limiting (config.py)
```python
RATE_LIMITING = {
    "requests_per_second": 4,           # Tokens per second
    "max_retries": 5,                    # Retry attempts per movie
    "initial_backoff": 1,                # Initial wait time (seconds)
    "max_backoff": 32,                   # Maximum wait time (seconds)
    "circuit_breaker_threshold": 5,      # Failures before circuit opens
    "circuit_breaker_timeout": 60        # Recovery time (seconds)
}

OPERATION_TIMEOUT = 600                  # Global timeout (10 minutes)
```

### Session Configuration (config.py)
```python
SESSION_CONFIG = {
    "connector_limit": 4,                # Max total connections
    "connector_limit_per_host": 10,      # Max per-host connections
    "timeout_total": 30,                 # Total request timeout
    "timeout_sock_read": 15,             # Read timeout
    "enable_cleanup_closed": True        # Clean up closed connections
}
```

---

## API Error Handling

| Status Code | Behavior |
|------------|----------|
| **200** | Success; validate response; record success |
| **404** | Movie not found; skip (no retry) |
| **429** | Rate limited; record failure; exponential backoff |
| **5xx** | Server error; record failure; exponential backoff |
| **Timeout** | Connection timeout; record failure; exponential backoff |

---

## Data Storage

- **Raw data**: `data/raw/movies.parquet` (Parquet binary format)
  - Why Parquet? Smaller than CSV, faster reads, type-safe
- **Cleaned data**: `data/processed/movies_cleaned.csv` (CSV for Excel compatibility)
- **Failed movies**: `data/raw/failed_movies.csv` (for auditing and retry)

---

## Testing the Pipeline

### Quick test with subset of movies
Edit `config.py` to use 3-5 movie IDs:
```python
MOVIE_IDS = [299534, 19995, 140607]  # 3 most popular movies
```

Expected time: ~2-3 seconds

### Full production run
Keep all movie IDs in `config.py` (or add more)

Expected time: ~4-5 seconds for 18 movies (~3.7s API calls + 1.5s processing)

---

## Troubleshooting

### "API_KEY environment variable not found"
**Solution**: Create `.env` file with `API_KEY=your_key`

### "Circuit breaker: OPEN"
**Reason**: Too many API failures (5+ in a row)
**Solution**: Check TMDB API status; wait 60s; retry

### "Validation failed for movie 12345"
**Reason**: Corrupted or malformed API response
**Action**: Check logs; movie saved to `failed_movies.csv` for manual review

### "Pipeline exceeded operation timeout"
**Reason**: Network too slow or TMDB API down
**Solution**: Increase `OPERATION_TIMEOUT` in `config.py` or reduce movie count

---

## Logs & Monitoring

All logs are saved to `logs/movie_pipeline.log` (also printed to console).

**Key metrics to monitor:**
- **Success rate**: Should be 100% (or 95%+ acceptable)
- **Rate**: Should be ~4.8 req/s (respects 4 req/s limit)
- **Circuit breaks**: Should be 0 (indicates API issues)
- **Blocked requests**: Should be 0% (indicates no rate limit violations)

---

## Future Enhancements

- [ ] Caching API responses to avoid duplicate fetches
- [ ] Track TMDB's `X-RateLimit-*` headers for proactive throttling
- [ ] Archive historical data (versioning)
- [ ] Unit tests for rate limiter, circuit breaker, validators
- [ ] Email alerts on pipeline failures
- [ ] Dashboard for real-time monitoring

---

## Notes

- **API Key Safety**: Never commit `.env` or `config.py`
- **Rate Limits**: TMDB has per-IP limit (40 req/10s), not per-key
- **Data Format**: Raw data in Parquet (binary); cleaned data in CSV (human-readable)
- **Retry Strategy**: Exponential backoff with jitter prevents thundering herd
- **Visualization**: Generated on every run with timestamp (no overwrites)

---
