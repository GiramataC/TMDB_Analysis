# TMDB Movie Analysis - Production Report

**Generated:** 2026-04-09 23:12:45

---

## Executive Summary


**Pipeline Status:** ✅ COMPLETED

**Movies Processed:** 18/18 (100.0% success rate)

**Processing Time:** 3.7s

**API Rate:** 4.83 req/s


### Key Findings
- **Total Movies Analyzed:** 18
- **Average ROI:** 8.17x
- **Average Profit:** $1478.1M
- **Data Quality Score:** 100.0%


---

## 1. Data Retrieval & Quality

### Retrieval Statistics

| Metric | Value |

|--------|-------|

| Requested | 18 |

| Successful | 18 |

| Failed | 0 |

| Success Rate | 100.0% |

| API Rate | 4.83 req/s |

| Elapsed Time | 3.7s |

| Circuit Breaks | 0 |


### Data Quality Checks

**Data Shape Progression:**

- Raw API data: 18 rows × 30 columns

- After cleaning: 18 rows × 22 columns

- After KPI engineering: 18 rows × 24 columns


**Missing Values (Cleaned Dataset):**

- ✅ No missing values



**Duplicates:** 0 (0 is good)


---

## 2. KPI Analysis

### Overall Statistics

| KPI | Min | Max | Mean | Median | Std Dev |

|-----|-----|-----|------|--------|----------|

| Budget ($M) | $125.0 | $356.0 | $213.8 | $200.0 | $62.0 |

| Revenue ($M) | $1243.2 | $2923.7 | $1691.8 | $1484.5 | $521.1 |

| Profit ($M) | $1032.7 | $2686.7 | $1478.1 | $1301.2 | $488.7 |

| ROI | 4.44x | 12.34x | 8.17x | 7.89x | 2.12x |

| Rating | 6.5 | 8.2 | 7.4 | 7.3 | 0.52 |


### Top 5 Movies by Revenue

1. **Avatar** — $2923.7M (ROI: 12.34x, Rating: 7.6)

2. **Avengers: Endgame** — $2799.4M (ROI: 7.86x, Rating: 8.2)

3. **Titanic** — $2264.2M (ROI: 11.32x, Rating: 7.9)

4. **Star Wars: The Force Awakens** — $2068.2M (ROI: 8.44x, Rating: 7.2)

5. **Avengers: Infinity War** — $2052.4M (ROI: 6.84x, Rating: 8.2)



### Top 5 Movies by ROI

1. **Avatar** — 12.34x ROI (Revenue: $2923.7M, Rating: 7.6)

2. **Titanic** — 11.32x ROI (Revenue: $2264.2M, Rating: 7.9)

3. **Jurassic World** — 11.14x ROI (Revenue: $1671.5M, Rating: 6.7)

4. **Harry Potter and the Deathly Hallows: Part 2** — 10.73x ROI (Revenue: $1341.5M, Rating: 8.1)

5. **Frozen II** — 9.69x ROI (Revenue: $1453.7M, Rating: 7.2)



---

## 3. Visualization Insights

### Chart 1: Revenue vs Budget Scatter Plot


**Chart:** Interactive scatter plot showing relationship between budget and revenue.

**Key Insights:**

- **Correlation:** 0.566 (strong positive relationship)

- **High-Budget Blockbusters:** 3 films with both high budget AND high revenue

- **Profitable Films:** 18/18 (100.0%) achieved positive ROI

- **Breaking Even Point:** Budget ≈ $nanM


### Chart 2: ROI Distribution by Genre


**Chart:** Box plot showing ROI variability across different movie genres.

**Key Insights:**

- **Best Performing Genre:** Comedy (avg ROI: 9.69x)

- **Most Consistent Genre:** Fantasy (lowest variance)

- **Total Genres Analyzed:** 11


### Chart 3: Popularity vs Rating


**Chart:** Scatter plot comparing popularity metrics against audience rating.

**Key Insights:**

- **Correlation:** 0.437

- **Highly Rated Films (≥8.0):** 3 movies

- **Average Popularity Score:** 23.98


### Chart 4: Yearly Box Office Trends


**Chart:** Line plot showing total revenue trends across release years.

**Key Insights:**

- **Peak Revenue Year:** 2015 ($6660.6M)

- **Year Range:** 1997 - 2019

- **Trend:** Increasing over period


### Chart 5: Franchise vs Standalone Performance


**Chart:** Bar chart comparing average performance metrics between franchise and standalone films.

**Key Insights:**

- **Franchise Avg Revenue:** $1682.7M

- **Franchise Avg ROI:** 7.99x

- **Standalone Avg Revenue:** $1765.1M

- **Standalone Avg ROI:** 9.62x

- **Better ROI:** Standalone films



---

## 4. Recommendations

### 🎯 Production Insights

- **Optimal Budget Range:** $160.0M - $240.0M (based on profitable films)

- **Best Performing Genre:** Comedy consistently delivers high ROI

- **Quality Matters:** High-rated films (8.0+) achieve 8.48x avg ROI


### ⚠️ Risk Factors

- **Investment Risk:** 0 films failed to break even (0.0%)

- **Average Shortfall:** $nanM loss for unprofitable films


---

## 5. Technical Details

### Data Pipeline Summary


- **API Source:** TMDB v3 REST API
- **Data Format:** Parquet (raw), CSV (clean)
- **Validation:** 3-stage Pydantic validation
- **Processing:** Async with 4 concurrent workers
- **Rate Limiting:** 4 req/s with circuit breaker
- **Error Handling:** Comprehensive with failed movie tracking

### Validation Results

- **API Response Validation:** Pass

- **Data Quality Checks:** Pass

- **KPI Calculations:** Pass (no inf/NaN)

- **Overall Status:** ✅ Production Ready


---

## Report Metadata


| Field | Value |
|-------|-------|
| Generated | 2026-04-09 23:12:45 |
| Data Points | 18 |
| Pipeline Status | ✅ Success |
| Quality Score | 100.0% |
| Next Run | Scheduled for tomorrow |

**Report Location:** `report.md`
**Visualizations:** `visualizations/` directory
**Raw Data:** `data/raw/movies.parquet`
**Logs:** `logs/movie_pipeline.log`
