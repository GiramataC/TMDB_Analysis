import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import os
import logging

# Setup logging
logger = logging.getLogger(__name__)

# Global style
plt.rcParams["figure.figsize"] = (8, 5)
plt.rcParams["axes.grid"] = True
plt.rcParams["grid.alpha"] = 0.3
plt.rcParams["grid.linestyle"] = "--"


# ===============================
# Helper: Save visualizations
# ===============================
def setup_visualization_dir():
    """Create visualizations directory if it doesn't exist"""
    os.makedirs("visualizations", exist_ok=True)
    return "visualizations"


# -------------------------------
# 1. Revenue vs Budget
# -------------------------------

def plot_revenue_vs_budget(df):
    """Save Revenue vs Budget scatter plot as interactive HTML"""
    fig = px.scatter(
        df,
        x="budget_musd",
        y="revenue_musd",
        hover_name="title",
        title="Revenue vs Budget",
    )
    
    fig.update_traces(marker=dict(size=8))
    
    filepath = "visualizations/01_revenue_vs_budget.html"
    fig.write_html(filepath)
    logger.info(f"Saved visualization: {filepath}")
    return filepath
    



# -------------------------------
# 2. ROI Distribution by Genre
# -------------------------------


def plot_roi_by_genre(df):
    """Save ROI Distribution by Genre as interactive HTML"""
    genre_df = df.copy()

    # Drop rows where genres is missing before splitting
    genre_df = genre_df[genre_df["genres"].notna()]
    genre_df = genre_df[genre_df["genres"] != "Unknown"]

    # Split then explode
    genre_df["genres"] = genre_df["genres"].str.split("|")
    genre_df = genre_df.explode("genres")

    # Clean up any stray whitespace
    genre_df["genres"] = genre_df["genres"].str.strip()

    # Keep valid ROI
    genre_df = genre_df[genre_df["roi"].notna()]

    fig = px.box(
        genre_df,
        x="genres",
        y="roi",
        title="ROI Distribution by Genre",
        points="all"  # show individual data points (very useful!)
    )

    # Rotate x-axis labels for readability
    fig.update_layout(
        xaxis_title="Genre",
        yaxis_title="ROI",
        xaxis_tickangle=45
    )

    filepath = "visualizations/02_roi_by_genre.html"
    fig.write_html(filepath)
    logger.info(f"Saved visualization: {filepath}")
    return filepath


# -------------------------------
# 3. Popularity vs Rating
# -------------------------------

def plot_popularity_vs_rating(df):
    """Save Popularity vs Rating scatter plot as PNG"""
    plt.figure()
    plt.scatter(df["popularity"], df["vote_average"])

    plt.xlabel("Popularity")
    plt.ylabel("Rating")
    plt.title("Popularity vs Rating")

    filepath = "visualizations/03_popularity_vs_rating.png"
    plt.savefig(filepath, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved visualization: {filepath}")
    return filepath


# -------------------------------
# 4. Yearly Revenue Trend
# -------------------------------

def plot_yearly_revenue(df):
    """Save Yearly Box Office Trends as PNG"""
    df = df.copy()

    df["year"] = df["release_date"].dt.year

    yearly = df.groupby("year")["revenue_musd"].sum()

    plt.figure()
    yearly.plot()

    plt.xlabel("Year")
    plt.ylabel("Total Revenue (Million USD)")
    plt.title("Yearly Box Office Trends")

    filepath = "visualizations/04_yearly_revenue.png"
    plt.savefig(filepath, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved visualization: {filepath}")
    return filepath


# -------------------------------
# 5. Franchise vs Standalone Comparison
# -------------------------------

def plot_franchise_vs_standalone(df):
    """Save Franchise vs Standalone Performance as PNG"""
    df = df.copy()

    # Create movie type
    df["movie_type"] = df["belongs_to_collection"].apply(
        lambda x: "Franchise" if x != "Unknown" else "Standalone"
    )

    # Aggregate
    comparison = df.groupby("movie_type")[
        ["revenue_musd", "roi", "vote_average"]
    ].mean()

    plt.figure()
    comparison.plot(kind="bar")

    plt.title("Franchise vs Standalone Performance")
    plt.ylabel("Average Value")
    plt.xticks(rotation=0)

    filepath = "visualizations/05_franchise_vs_standalone.png"
    plt.savefig(filepath, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved visualization: {filepath}")
    return filepath