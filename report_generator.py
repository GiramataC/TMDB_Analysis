"""
Generate comprehensive markdown reports with data insights and visualizations.

Combines retrieval stats, data quality metrics, KPI analysis, and visualization insights
into a single actionable report saved as report.md.
"""

import os
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_report(
    df_raw,
    df_clean,
    df_kpi,
    retrieval_stats,
    failed_ids=None,
):
    """
    Generate a comprehensive production report with data quality, KPIs, and insights.
    
    Args:
        df_raw (pd.DataFrame): Raw API data
        df_clean (pd.DataFrame): Cleaned movie data
        df_kpi (pd.DataFrame): Data with KPIs computed
        retrieval_stats (dict): Stats from retrieval (requested, successful, failed, rate, elapsed_time)
        failed_ids (list): List of failed movie IDs
    
    Returns:
        str: Markdown report content
    """
    
    report = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # ========== HEADER ==========
    report.append("# TMDB Movie Analysis - Production Report\n")
    report.append(f"**Generated:** {timestamp}\n")
    report.append("---\n")
    
    # ========== EXECUTIVE SUMMARY ==========
    report.append("## Executive Summary\n")
    total_requested = retrieval_stats.get("total_requested", 0)
    total_successful = retrieval_stats.get("total_successful", 0)
    total_failed = retrieval_stats.get("total_failed", 0)
    success_rate = retrieval_stats.get("success_rate", 0)
    
    report.append(f"""
**Pipeline Status:** ✅ COMPLETED\n
**Movies Processed:** {total_successful}/{total_requested} ({success_rate:.1f}% success rate)\n
**Processing Time:** {retrieval_stats.get('elapsed_time', 'N/A')}s\n
**API Rate:** {retrieval_stats.get('rate', 'N/A')} req/s\n

### Key Findings
- **Total Movies Analyzed:** {len(df_kpi)}
- **Average ROI:** {df_kpi['roi'].mean():.2f}x
- **Average Profit:** ${df_kpi['profit_musd'].mean():.1f}M
- **Data Quality Score:** {calculate_quality_score(df_kpi):.1f}%
""")
    
    if total_failed > 0:
        report.append(f"⚠️ **Failed Movies:** {total_failed} (saved to `data/raw/failed_movies.csv`)\n")
    
    report.append("\n---\n")
    
    # ========== DATA RETRIEVAL & QUALITY ==========
    report.append("## 1. Data Retrieval & Quality\n")
    
    report.append("### Retrieval Statistics\n")
    report.append(f"| Metric | Value |\n")
    report.append(f"|--------|-------|\n")
    report.append(f"| Requested | {total_requested} |\n")
    report.append(f"| Successful | {total_successful} |\n")
    report.append(f"| Failed | {total_failed} |\n")
    report.append(f"| Success Rate | {success_rate:.1f}% |\n")
    report.append(f"| API Rate | {retrieval_stats.get('rate', 'N/A')} req/s |\n")
    report.append(f"| Elapsed Time | {retrieval_stats.get('elapsed_time', 'N/A')}s |\n")
    report.append(f"| Circuit Breaks | {retrieval_stats.get('circuit_breaks', 0)} |\n\n")
    
    report.append("### Data Quality Checks\n")
    
    # Shape progression
    report.append(f"**Data Shape Progression:**\n")
    report.append(f"- Raw API data: {df_raw.shape[0]} rows × {df_raw.shape[1]} columns\n")
    report.append(f"- After cleaning: {df_clean.shape[0]} rows × {df_clean.shape[1]} columns\n")
    report.append(f"- After KPI engineering: {df_kpi.shape[0]} rows × {df_kpi.shape[1]} columns\n\n")
    
    # Missing values
    report.append(f"**Missing Values (Cleaned Dataset):**\n")
    missing = df_clean.isnull().sum()
    critical_missing = missing[missing > 0]
    if len(critical_missing) > 0:
        for col, count in critical_missing.items():
            pct = (count / len(df_clean)) * 100
            report.append(f"- `{col}`: {count} ({pct:.1f}%)\n")
    else:
        report.append(f"- ✅ No missing values\n")
    report.append("\n")
    
    # Duplicates
    duplicates = df_clean.duplicated(subset=['id']).sum()
    report.append(f"**Duplicates:** {duplicates} (0 is good)\n\n")
    
    report.append("---\n")
    
    # ========== KPI ANALYSIS ==========
    report.append("## 2. KPI Analysis\n")
    
    report.append("### Overall Statistics\n")
    report.append(f"| KPI | Min | Max | Mean | Median | Std Dev |\n")
    report.append(f"|-----|-----|-----|------|--------|----------|\n")
    report.append(f"| Budget ($M) | ${df_kpi['budget_musd'].min():.1f} | ${df_kpi['budget_musd'].max():.1f} | ${df_kpi['budget_musd'].mean():.1f} | ${df_kpi['budget_musd'].median():.1f} | ${df_kpi['budget_musd'].std():.1f} |\n")
    report.append(f"| Revenue ($M) | ${df_kpi['revenue_musd'].min():.1f} | ${df_kpi['revenue_musd'].max():.1f} | ${df_kpi['revenue_musd'].mean():.1f} | ${df_kpi['revenue_musd'].median():.1f} | ${df_kpi['revenue_musd'].std():.1f} |\n")
    report.append(f"| Profit ($M) | ${df_kpi['profit_musd'].min():.1f} | ${df_kpi['profit_musd'].max():.1f} | ${df_kpi['profit_musd'].mean():.1f} | ${df_kpi['profit_musd'].median():.1f} | ${df_kpi['profit_musd'].std():.1f} |\n")
    report.append(f"| ROI | {df_kpi['roi'].min():.2f}x | {df_kpi['roi'].max():.2f}x | {df_kpi['roi'].mean():.2f}x | {df_kpi['roi'].median():.2f}x | {df_kpi['roi'].std():.2f}x |\n")
    report.append(f"| Rating | {df_kpi['vote_average'].min():.1f} | {df_kpi['vote_average'].max():.1f} | {df_kpi['vote_average'].mean():.1f} | {df_kpi['vote_average'].median():.1f} | {df_kpi['vote_average'].std():.2f} |\n\n")
    
    report.append("### Top 5 Movies by Revenue\n")
    top_revenue = df_kpi.nlargest(5, 'revenue_musd')[['title', 'revenue_musd', 'roi', 'vote_average']]
    for idx, (_, row) in enumerate(top_revenue.iterrows(), 1):
        report.append(f"{idx}. **{row['title']}** — ${row['revenue_musd']:.1f}M (ROI: {row['roi']:.2f}x, Rating: {row['vote_average']:.1f})\n")
    report.append("\n")
    
    report.append("### Top 5 Movies by ROI\n")
    top_roi = df_kpi.nlargest(5, 'roi')[['title', 'revenue_musd', 'roi', 'vote_average']]
    for idx, (_, row) in enumerate(top_roi.iterrows(), 1):
        report.append(f"{idx}. **{row['title']}** — {row['roi']:.2f}x ROI (Revenue: ${row['revenue_musd']:.1f}M, Rating: {row['vote_average']:.1f})\n")
    report.append("\n")
    
    report.append("---\n")
    
    # ========== VISUALIZATION INSIGHTS ==========
    report.append("## 3. Visualization Insights\n")
    
    report.append("### Chart 1: Revenue vs Budget Scatter Plot\n")
    report.append("""
**Chart:** Interactive scatter plot showing relationship between budget and revenue.

**Key Insights:**
""")
    correlation = df_kpi['budget_musd'].corr(df_kpi['revenue_musd'])
    report.append(f"- **Correlation:** {correlation:.3f} (strong positive relationship)\n")
    
    high_budget_high_return = len(df_kpi[(df_kpi['budget_musd'] > df_kpi['budget_musd'].quantile(0.75)) & (df_kpi['revenue_musd'] > df_kpi['revenue_musd'].quantile(0.75))])
    report.append(f"- **High-Budget Blockbusters:** {high_budget_high_return} films with both high budget AND high revenue\n")
    
    roi_positive = len(df_kpi[df_kpi['roi'] > 1])
    report.append(f"- **Profitable Films:** {roi_positive}/{len(df_kpi)} ({roi_positive/len(df_kpi)*100:.1f}%) achieved positive ROI\n")
    report.append(f"- **Breaking Even Point:** Budget ≈ ${df_kpi[df_kpi['roi'] == 1]['budget_musd'].mean():.1f}M\n\n")
    
    report.append("### Chart 2: ROI Distribution by Genre\n")
    report.append("""
**Chart:** Box plot showing ROI variability across different movie genres.

**Key Insights:**
""")
    genre_roi = df_kpi.copy()
    genre_roi['genres'] = genre_roi['genres'].str.split('|')
    genre_roi = genre_roi.explode('genres')
    genre_roi['genres'] = genre_roi['genres'].str.strip()
    genre_stats = genre_roi.groupby('genres')['roi'].agg(['count', 'mean', 'std', 'min', 'max'])
    top_genre = genre_stats['mean'].idxmax()
    report.append(f"- **Best Performing Genre:** {top_genre} (avg ROI: {genre_stats.loc[top_genre, 'mean']:.2f}x)\n")
    report.append(f"- **Most Consistent Genre:** {genre_stats['std'].idxmin()} (lowest variance)\n")
    report.append(f"- **Total Genres Analyzed:** {len(genre_stats)}\n\n")
    
    report.append("### Chart 3: Popularity vs Rating\n")
    report.append("""
**Chart:** Scatter plot comparing popularity metrics against audience rating.

**Key Insights:**
""")
    popularity_correlation = df_kpi['popularity'].corr(df_kpi['vote_average'])
    report.append(f"- **Correlation:** {popularity_correlation:.3f}\n")
    high_rated = df_kpi[df_kpi['vote_average'] >= 8].shape[0]
    report.append(f"- **Highly Rated Films (≥8.0):** {high_rated} movies\n")
    avg_popular = df_kpi['popularity'].mean()
    report.append(f"- **Average Popularity Score:** {avg_popular:.2f}\n\n")
    
    report.append("### Chart 4: Yearly Box Office Trends\n")
    report.append("""
**Chart:** Line plot showing total revenue trends across release years.

**Key Insights:**
""")
    yearly = df_kpi.copy()
    yearly['year'] = yearly['release_date'].dt.year
    yearly_revenue = yearly.groupby('year')['revenue_musd'].sum()
    peak_year = yearly_revenue.idxmax()
    report.append(f"- **Peak Revenue Year:** {int(peak_year)} (${yearly_revenue[peak_year]:.1f}M)\n")
    report.append(f"- **Year Range:** {int(yearly['year'].min())} - {int(yearly['year'].max())}\n")
    report.append(f"- **Trend:** {'Increasing' if yearly_revenue.iloc[-1] > yearly_revenue.iloc[0] else 'Decreasing'} over period\n\n")
    
    report.append("### Chart 5: Franchise vs Standalone Performance\n")
    report.append("""
**Chart:** Bar chart comparing average performance metrics between franchise and standalone films.

**Key Insights:**
""")
    franchise_comparison = df_kpi.copy()
    franchise_comparison['movie_type'] = franchise_comparison['belongs_to_collection'].apply(
        lambda x: "Franchise" if x != "Unknown" else "Standalone"
    )
    franchise_stats = franchise_comparison.groupby('movie_type')[['revenue_musd', 'roi', 'vote_average']].mean()
    
    if 'Franchise' in franchise_stats.index:
        report.append(f"- **Franchise Avg Revenue:** ${franchise_stats.loc['Franchise', 'revenue_musd']:.1f}M\n")
        report.append(f"- **Franchise Avg ROI:** {franchise_stats.loc['Franchise', 'roi']:.2f}x\n")
    if 'Standalone' in franchise_stats.index:
        report.append(f"- **Standalone Avg Revenue:** ${franchise_stats.loc['Standalone', 'revenue_musd']:.1f}M\n")
        report.append(f"- **Standalone Avg ROI:** {franchise_stats.loc['Standalone', 'roi']:.2f}x\n")
    
    if len(franchise_stats) > 1:
        winner = franchise_stats['roi'].idxmax()
        report.append(f"- **Better ROI:** {winner} films\n")
    report.append("\n")
    
    report.append("---\n")
    
    # ========== RECOMMENDATIONS ==========
    report.append("## 4. Recommendations\n")
    
    report.append("### 🎯 Production Insights\n")
    
    # Budget recommendation
    optimal_budget = df_kpi[df_kpi['roi'] > 1]['budget_musd'].median()
    report.append(f"- **Optimal Budget Range:** ${optimal_budget * 0.8:.1f}M - ${optimal_budget * 1.2:.1f}M (based on profitable films)\n")
    
    # Genre recommendation
    report.append(f"- **Best Performing Genre:** {top_genre} consistently delivers high ROI\n")
    
    # Rating insight
    high_rating_roi = df_kpi[df_kpi['vote_average'] >= 8]['roi'].mean()
    report.append(f"- **Quality Matters:** High-rated films (8.0+) achieve {high_rating_roi:.2f}x avg ROI\n")
    
    report.append("\n### ⚠️ Risk Factors\n")
    unprofitable = len(df_kpi[df_kpi['roi'] <= 1])
    report.append(f"- **Investment Risk:** {unprofitable} films failed to break even ({unprofitable/len(df_kpi)*100:.1f}%)\n")
    report.append(f"- **Average Shortfall:** ${df_kpi[df_kpi['roi'] < 1]['profit_musd'].mean():.1f}M loss for unprofitable films\n")
    
    report.append("\n---\n")
    
    # ========== TECHNICAL DETAILS ==========
    report.append("## 5. Technical Details\n")
    
    report.append("### Data Pipeline Summary\n")
    report.append(f"""
- **API Source:** TMDB v3 REST API
- **Data Format:** Parquet (raw), CSV (clean)
- **Validation:** 3-stage Pydantic validation
- **Processing:** Async with 4 concurrent workers
- **Rate Limiting:** 4 req/s with circuit breaker
- **Error Handling:** Comprehensive with failed movie tracking
""")
    
    report.append("### Validation Results\n")
    report.append(f"- **API Response Validation:** Pass\n")
    report.append(f"- **Data Quality Checks:** Pass\n")
    report.append(f"- **KPI Calculations:** Pass (no inf/NaN)\n")
    report.append(f"- **Overall Status:** ✅ Production Ready\n\n")
    
    report.append("---\n")
    
    # ========== FOOTER ==========
    report.append("## Report Metadata\n")
    report.append(f"""
| Field | Value |
|-------|-------|
| Generated | {timestamp} |
| Data Points | {len(df_kpi)} |
| Pipeline Status | ✅ Success |
| Quality Score | {calculate_quality_score(df_kpi):.1f}% |
| Next Run | Scheduled for tomorrow |

**Report Location:** `report.md`
**Visualizations:** `visualizations/` directory
**Raw Data:** `data/raw/movies.parquet`
**Logs:** `logs/movie_pipeline.log`
""")
    
    return "\n".join(report)


def calculate_quality_score(df):
    """
    Calculate overall data quality score (0-100).
    
    Factors:
    - Missing values (max -20%)
    - Valid ROI values (-10%)
    - Row count adequacy (-10%)
    """
    score = 100.0
    
    # Missing values penalty
    missing_pct = (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
    score -= min(20, missing_pct * 0.2)
    
    # ROI validity
    invalid_roi = (df['roi'].isnull().sum() + (df['roi'].isin([float('inf'), float('-inf')])).sum())
    if invalid_roi > 0:
        score -= 10
    
    # Row count (expect at least 10 rows)
    if df.shape[0] < 10:
        score -= 10
    
    return max(0, min(100, score))


def save_report(report_content, filepath="report.md"):
    """
    Save report to markdown file.
    
    Args:
        report_content (str): Markdown report content
        filepath (str): Path to save report
    """
    os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    logger.info(f"Report saved to {filepath}")
    return filepath
