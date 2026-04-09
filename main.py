
import os
import pandas as pd
import logging

# Import modules
from Data_retrieval import main as fetch_data
from Data_cleaning import clean_movies
from Analysis import add_kpis, top_revenue, highest_roi, top_directors
from visualisation import (
    setup_visualization_dir,
    plot_revenue_vs_budget,
    plot_roi_by_genre,
    plot_popularity_vs_rating,
    plot_yearly_revenue,
    plot_franchise_vs_standalone
)

# -------------------------------
# Setup logging
# -------------------------------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "movie_pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),  # ← fixed log path
        logging.StreamHandler()
    ]
)

RAW_PARQUET = "data/raw/movies.parquet"
CLEAN_PATH = "data/processed/movies_cleaned.csv"


# -------------------------------
# Main Pipeline
# -------------------------------
def main():

    logging.info("Starting Movie Data Pipeline")

    # -------------------------------
    # Step 1: Data Retrieval
    # -------------------------------
    logging.info("Fetching data from API (with strict rate limiting)...")
    try:
        fetch_data()
    except Exception as e:
        logging.exception(f"Data retrieval failed: {e}")
        return

    if not os.path.exists(RAW_PARQUET):
        logging.error("Raw data file not found. Exiting.")
        return

    # -------------------------------
    # Step 2: Load Raw Data
    # -------------------------------
    logging.info("Loading raw data...")
    df = pd.read_parquet(RAW_PARQUET)  # ← use parquet for consistency with notebook
    
    if df.empty:
        logging.error("No data retrieved from API. Exiting.")
        return

    # -------------------------------
    # Step 3: Data Cleaning
    # -------------------------------
    logging.info("Cleaning data...")
    df_clean = clean_movies(df)

    # Save cleaned data
    os.makedirs("data/processed", exist_ok=True)
    df_clean.to_csv(CLEAN_PATH, index=False)
    logging.info(f"Cleaned data saved to {CLEAN_PATH}")

    # -------------------------------
    # Step 4: KPI Engineering
    # -------------------------------
    logging.info("Adding KPIs...")
    df_kpi = add_kpis(df_clean)

    # -------------------------------
    # Step 5: Analysis Outputs
    # -------------------------------
    logging.info("Running analysis...")

    print("\nTop Revenue Movies")
    print(top_revenue(df_kpi))

    print("\nHighest ROI Movies")
    print(highest_roi(df_kpi))

    print("\nTop Directors")
    print(top_directors(df_kpi))

    # -------------------------------
    # Step 6: Visualizations
    # -------------------------------
    logging.info("Generating visualizations...")
    
    setup_visualization_dir()
    
    viz_files = []
    viz_files.append(plot_revenue_vs_budget(df_kpi))
    viz_files.append(plot_roi_by_genre(df_kpi))
    viz_files.append(plot_popularity_vs_rating(df_kpi))
    viz_files.append(plot_yearly_revenue(df_kpi))
    viz_files.append(plot_franchise_vs_standalone(df_kpi))
    
    logging.info(f"Saved {len(viz_files)} visualizations to visualizations/ directory")

    logging.info("Pipeline completed successfully!")


# -------------------------------
# Run
# -------------------------------
if __name__ == "__main__":
    main()