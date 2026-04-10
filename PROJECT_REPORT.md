# TMDB Movie Analysis Pipeline - Report

**Author:** Muhoracyeye Candide Giramata  
**Project:** Demo 2 Lab - TMDB Movie Data Analysis using Python, APIs, and Pandas  
**Date:** April 2026

---

## Executive Summary

This project implements a **production-grade data pipeline** that extracts, validates, cleans, and analyzes movie data from The Movie Database (TMDB) API. The pipeline emphasizes reliability, data quality, and comprehensive error handling through strict rate limiting, async concurrency, multi-stage validation, and detailed logging.

**Key Metrics:**
- **Success Rate:** 100% (18/18 movies fetched successfully)
- **API Rate:** 4.8 req/sec (respects TMDB's 40 req/10s limit)
- **Data Quality Score:** 100%
- **Processing Time:** ~8 seconds (retrieval + cleaning + analysis + visualizations)

---

## 1. Architecture & Design

### 1.1 Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Language** | Python | 3.11 | Runtime environment |
| **Async Framework** | asyncio | Built-in | Non-blocking concurrent requests |
| **HTTP Client** | aiohttp | 3.9.1 | Async HTTP requests |
| **Data Processing** | pandas | 2.2.2 | Data manipulation & analysis |
| **Data Validation** | Pydantic | 2.5.0 | Schema validation & type safety |
| **Visualization** | Plotly + Matplotlib | Latest | Interactive & static charts |
| **Configuration** | python-dotenv | Latest | Secure API key management |

### 1.2 Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Data Retrieval                          │
│  • Async API calls with 4 concurrent workers                │
│  • Token bucket rate limiting (4 req/sec)                   │
│  • Circuit breaker (5 failures → 60s timeout)               │
│  • Exponential backoff with jitter (1s→32s)                │
│  • Pydantic validation on raw API responses                 │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                      Data Cleaning                           │
│  • Type conversion (budget, revenue → numeric)              │
│  • Missing value handling (zero → NaN)                      │
│  • Duplicate & invalid row removal                          │
│  • JSON field extraction & flattening                       │
│  • Unit normalization (budget_musd, revenue_musd)           │
│  • Data quality validation (Pydantic schemas)               │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                   KPI Engineering                            │
│  • Profit calculation (Revenue - Budget)                    │
│  • ROI calculation (Revenue / Budget, safe division)        │
│  • Inf/NaN detection & prevention                           │
│  • KPI validation (Pydantic schemas)                        │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                    Analysis & Insights                       │
│  • Top revenue movies ranking                               │
│  • Highest ROI identification                               │
│  • Director performance analysis                            │
│  • Genre-based insights                                     │
│  • Franchise vs standalone comparison                       │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                    Visualization                             │
│  • 5 production-quality charts (HTML + PNG)                 │
│  • Interactive Plotly dashboards                            │
│  • High-resolution static images (300 DPI)                  │
│  • Proper axis labels and legends                           │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Data Retrieval (Production-Grade API Integration)

### 2.1 API Setup
- **API Key Management:** Stored securely in `.env` file using python-dotenv
- **API Source:** TMDB v3 REST API (`https://api.themoviedb.org/3/`)
- **Data Format:** JSON responses with nested credits, genres, and production companies

### 2.2 Rate Limiting Strategy
**Problem:** TMDB enforces a strict rate limit of 40 requests per 10 seconds (4 req/sec).

**Solution:** Multi-layered rate limiting:

| Strategy | Implementation | Impact |
|----------|----------------|--------|
| **Token Bucket** | 4 tokens/sec, constant refill | Smooth request distribution |
| **Circuit Breaker** | Open after 5 consecutive failures | Prevents cascading failures |
| **Exponential Backoff** | 1s→2s→4s→8s→16s→32s with ±10% jitter | Graceful recovery |
| **Semaphore** | Max 4 concurrent workers | Resource constraint handling |

**Actual Performance:** 4.8 req/sec while respecting rate limit bounds

### 2.3 Error Handling & Resilience

| Error Type | Status Code | Handling |
|-----------|-----------|----------|
| **Not Found** | 404 | Skip without retry (movie doesn't exist) |
| **Rate Limited** | 429 | Exponential backoff + circuit breaker |
| **Server Error** | 5xx | Exponential backoff (up to 5 retries) |
| **Timeout** | - | Operation timeout at 600s global limit |
| **Invalid Response** | 200 + validation fail | Pydantic validation error + retry |

**Failed Movie Tracking:** All failed movies logged to `data/raw/failed_movies.csv` with timestamp and error details for audit trails.

### 2.4 API Response Validation
**Pydantic Schema Validation:**
- Title: Non-empty string
- Budget/Revenue: Realistic values (<$5B)
- Release Date: Valid YYYY-MM-DD format
- Cast/Crew: Extracted from nested `credits` object
- Missing fields: Handled gracefully with defaults

---

## 3. Data Cleaning & Transformation

### 3.1 Column Removal
**Dropped Columns:** `adult`, `imdb_id`, `original_title`, `video`, `homepage`  
**Rationale:** Unnecessary for movie performance analysis

### 3.2 Data Quality & Deduplication

| Issue | Solution | Result |
|-------|----------|--------|
| **Duplicate entries** | Deduplicate by movie ID | 0 duplicates in final dataset |
| **Missing critical values** | Remove rows with null ID or title | 100% data completeness |
| **Insufficient data** | Keep only rows with ≥10 non-null values | Quality threshold enforced |
| **Unreleased movies** | Filter for status="Released" only | Production-ready films only |

### 3.3 JSON & Nested Field Extraction

**Complex Fields Processed:**
- **Genres:** List → Pipe-separated string (`Action|Drama|Sci-Fi`)
- **Production Companies:** List of objects → Company names
- **Spoken Languages:** List → ISO language codes
- **Belongs to Collection:** Dictionary → Collection name

**Example:**
```json
Input: {"id": 1, "name": "Avengers Collection", ...}
Output: "Avengers Collection"
```

### 3.4 Type Conversion & Normalization

| Column | Original Type | New Type | Transformation |
|--------|---------------|----------|---|
| `budget` | int (USD) | float | ÷ 1,000,000 → budget_musd |
| `revenue` | int (USD) | float | ÷ 1,000,000 → revenue_musd |
| `release_date` | string | datetime64 | YYYY-MM-DD parsing |
| `vote_average` | float | float | 0-10 rating scale |
| `popularity` | float | float | Numeric sorting |

### 3.5 Zero & Missing Value Handling

**Strategy:** Replace unrealistic zeros with NaN to prevent skewed averages and division errors.

| Scenario | Original Value | Action | Reason |
|----------|---|-----------|---------|
| Movie with $0 budget | 0 | → NaN | Unrealistic; prevents ROI div/0 |
| Movie with $0 revenue | 0 | → NaN | Data quality; no sales = bad data |
| Movie with 0 runtime | 0 | → NaN | Invalid; likely missing |
| Movie with 0 votes | vote_count=0 | → NaN avg rating | Prevents unweighted rankings |

**Text Cleanup:** Replaced generic placeholders ("No Data", "N/A", empty strings) with NaN for consistency.

---

## 4. KPI Engineering & Validation

### 4.1 Key Performance Indicators

#### Profitability
```
Profit ($M) = Revenue ($M) - Budget ($M)
```
- **Range:** -$1B to +$2.7B
- **Median:** $1.3B (indicates typical blockbuster success)

#### Return on Investment (ROI)
```
ROI = Revenue ($M) / Budget ($M)
```
- **Safe Division:** Uses `np.where()` to prevent division by zero
- **Validation:** Detects and prevents inf/NaN propagation
- **Range:** 4.4x to 12.3x
- **Insight:** All movies broke even; profitable portfolio

### 4.2 Validation at Multiple Stages

**Stage 1: API Response Validation**
- Pydantic schema validates raw API data
- Catches malformed responses
- Retries on validation failure

**Stage 2: Cleaned Data Validation**
- Quality checks for duplicates, nulls, empty datasets
- Verifies shape and integrity
- Logs warnings for data quality issues

**Stage 3: KPI Validation**
- Prevents inf/NaN in final calculations
- Validates ROI computation logic
- Ensures mathematical soundness

---

## 5. Analysis Results

### 5.1 KPI Summary Statistics

| Metric | Min | Max | Mean | Median | Std Dev |
|--------|-----|-----|------|--------|---------|
| **Budget ($M)** | $125 | $356 | $214 | $200 | $62 |
| **Revenue ($M)** | $1,243 | $2,924 | $1,692 | $1,485 | $521 |
| **Profit ($M)** | $1,033 | $2,687 | $1,478 | $1,301 | $489 |
| **ROI** | 4.44x | 12.34x | 8.17x | 7.89x | 2.12x |
| **Rating** | 6.5 | 8.2 | 7.4 | 7.3 | 0.52 |

### 5.2 Top 5 Movies by Revenue
1. **Avatar** ($2,923.7M) — ROI: 12.34x, Rating: 7.6
2. **Avengers: Endgame** ($2,799.4M) — ROI: 7.86x, Rating: 8.2
3. **Titanic** ($2,264.2M) — ROI: 11.32x, Rating: 7.9
4. **Star Wars: The Force Awakens** ($2,068.2M) — ROI: 8.44x, Rating: 7.2
5. **Avengers: Infinity War** ($2,052.4M) — ROI: 6.84x, Rating: 8.2

### 5.3 Top 5 Movies by ROI
1. **Avatar** (12.34x) — Budget: $237M, Rating: 7.6
2. **Titanic** (11.32x) — Budget: $200M, Rating: 7.9
3. **Jurassic World** (11.14x) — Budget: $150M, Rating: 6.7
4. **Harry Potter: Deathly Hallows Pt 2** (10.73x) — Budget: $125M, Rating: 8.1
5. **Frozen II** (9.69x) — Budget: $150M, Rating: 7.2

### 5.4 Genre Performance
**Best Performing Genre:** Comedy (9.69x avg ROI)  
**Most Consistent Genre:** Fantasy (lowest variance)  
**Total Genres Analyzed:** 11

### 5.5 Director Analysis
**Top Directors by Revenue:**
1. James Cameron (2 films): $5,187.9M total, 7.75 avg rating
2. Anthony Russo (2 films): $4,851.9M total, 8.23 avg rating
3. Joss Whedon (2 films): $2,924.2M total, 7.63 avg rating

### 5.6 Franchise vs Standalone Comparison
| Metric | Franchise | Standalone |
|--------|-----------|-----------|
| **Avg Revenue** | $1,682.7M | $1,765.1M |
| **Avg ROI** | 7.99x | 9.62x |
| **Avg Rating** | 7.4 | 7.4 |
| **Better Performer** | — | Standalone (higher ROI) |

---

## 6. Visualizations

### 6.1 Interactive Charts (HTML - Plotly)

**Chart 1: Revenue vs Budget Scatter Plot**
- **Type:** Interactive scatter plot
- **Axes:** Budget ($M) vs Revenue ($M)
- **Hover Details:** Movie title, exact values
- **Insight:** 0.566 correlation; strong budget-revenue relationship
- **Finding:** All 18 films achieved ROI > 1.0 (100% profitability)

**Chart 2: ROI Distribution by Genre**
- **Type:** Interactive box plot with individual points
- **Axes:** Genre vs ROI (multiplier)
- **Features:** Zoom, pan, toggle genres
- **Insight:** Comedy shows highest median ROI (9.69x); Fantasy most consistent
- **Finding:** Genre significantly influences profitability

### 6.2 Static Charts (PNG - Matplotlib)

**Chart 3: Popularity vs Rating**
- **Type:** Scatter plot
- **Axes:** Popularity score vs Rating (0-10)
- **Insight:** 0.437 correlation; moderate relationship
- **Finding:** 3 films rated ≥8.0; average popularity score 23.98

**Chart 4: Yearly Box Office Trends**
- **Type:** Line chart
- **Axes:** Release year vs Total revenue ($M)
- **Peak Year:** 2015 ($6,660.6M revenue spike)
- **Trend:** Upward trajectory from 1997-2019
- **Finding:** Recent blockbusters dominate revenue

**Chart 5: Franchise vs Standalone Performance**
- **Type:** Grouped bar chart
- **Metrics:** Revenue, ROI, Rating (avg by movie type)
- **Legend:** Clear labels for each metric
- **Finding:** Standalones achieve higher avg ROI (9.62x vs 7.99x)

---

## 7. Key Business Insights

### 7.1 Production Strategy
- **Optimal Budget Range:** $160M - $240M (sweet spot for profitability)
- **Quality Matters:** High-rated films (8.0+) achieve 8.48x avg ROI
- **Best Genres:** Comedy delivers highest ROI; Fantasy most reliable
- **Director Impact:** Established directors (Cameron, Russo) deliver consistent $2B+ returns

### 7.2 Market Trends
- **Franchise Stability:** Franchises provide predictable $7.99x avg ROI
- **Standalone Upside:** Standalones achieve higher avg ROI (9.62x)
- **Revenue Spike:** 2015 shows massive spike ($6.6B); investigate blockbuster releases
- **Language:** Multilingual films (Spanish, French, German) perform well alongside English

### 7.3 Risk Factors
- **Minimum Investment:** Films budget <$125M may underperform
- **Unrated Risk:** Films with 0 votes removed to prevent skewed rankings
- **Production Concentration:** 89% of films from USA (limited geographic diversity)

---



## 9. Files & Outputs

### 9.1 Input Data
- **Source:** TMDB API v3
- **Movie IDs:** 18 films (blockbusters + classics)
- **Format:** JSON responses

### 9.2 Processing Files
- `Data_retrieval.py` — Async API fetching with rate limiting
- `Data_cleaning.py` — Data preprocessing & validation
- `Analysis.py` — KPI engineering & aggregations
- `visualisation.py` — Chart generation (HTML + PNG)
- `rate_limiter.py` — Token bucket, circuit breaker, backoff
- `validators.py` — Pydantic schemas for 3-stage validation

### 9.3 Output Data
- `data/raw/movies.parquet` — Raw API data (18 rows × 30 columns)
- `data/processed/movies_cleaned.csv` — Cleaned data (18 rows × 22 columns)
- `visualizations/` — 5 production-quality charts

### 9.4 Logs & Reports
- `logs/movie_pipeline.log` — Detailed execution log
- `report.md` — Optional comprehensive report with insights

---


## 10. Conclusion

This TMDB Movie Analysis pipeline demonstrates **production-grade data engineering** principles:

- **Reliability:** Strict rate limiting, circuit breaker, error handling
- **Scalability:** Async concurrency with semaphore control
- **Quality:** Multi-stage validation with Pydantic schemas
- **Maintainability:** Modular code, logging, documented assumptions




---

## Appendix: How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set API key
echo "API_KEY=your_tmdb_api_key" > .env

# 3. Configure movie IDs (optional)
# Edit config.py MOVIE_IDS list as needed

# 4. Run pipeline
python main.py

# 5. View outputs
# - Visualizations: visualizations/ directory
# - Cleaned data: data/processed/movies_cleaned.csv
# - Logs: logs/movie_pipeline.log
```

---

