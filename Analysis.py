import pandas as pd
import numpy as np
import logging
from validators import check_dataframe_quality

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------------
# KPI Feature Engineering
# -------------------------------

def add_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add KPI columns like profit and ROI with validation.
    
    Handles edge cases:
    - Division by zero (budget = 0)
    - Missing values (NaN budget/revenue)
    - Invalid calculated values (inf, NaN)
    """
    df = df.copy()

    if "revenue_musd" in df.columns and "budget_musd" in df.columns:
        df["profit_musd"] = df["revenue_musd"] - df["budget_musd"]
        
        # ROI = revenue / budget
        # Handle division by zero: if budget is 0 or NaN, ROI should be NaN
        df["roi"] = np.where(
            (df["budget_musd"] > 0) & (df["budget_musd"].notna()),
            df["revenue_musd"] / df["budget_musd"],
            np.nan
        )
        
        # Validate profit and ROI
        invalid_roi = df["roi"].isin([np.inf, -np.inf])
        if invalid_roi.any():
            logging.warning(f"Found {invalid_roi.sum()} infinite ROI values (budget was 0). Converting to NaN.")
            df.loc[invalid_roi, "roi"] = np.nan
        
        invalid_profit = df["profit_musd"].isin([np.inf, -np.inf])
        if invalid_profit.any():
            logging.warning(f"Found {invalid_profit.sum()} infinite profit values. Converting to NaN.")
            df.loc[invalid_profit, "profit_musd"] = np.nan

    # Validate KPI data quality
    try:
        check_dataframe_quality(df, "Dataset with KPIs")
    except ValueError as e:
        logging.warning(f"KPI data quality warning: {e}")

    return df


# -------------------------------
# Universal Ranking UDF
# -------------------------------

def rank_movies(df, metric, top=True, n=10, filter_dict=None):
    """
    Rank movies based on a specified metric with flexible filtering.

    Parameters:
    - metric: column to rank on
    - top: True (highest), False (lowest)
    - n: number of results
    - filter_dict: dictionary of filters
        Supports:
        - Numeric filters: ">=10", "<5", etc.
        - String filters: "Action"
        - Multi-values: ["Action", "Sci-Fi"]
    """

    filtered_df = df.copy()

    # -------------------------------
    # Apply filters
    # -------------------------------
    if filter_dict:
        for col, cond_val in filter_dict.items():

            # MULTI-VALUE FILTER (list)
            if isinstance(cond_val, list):
                for val in cond_val:
                    filtered_df[col] = filtered_df[col].astype(str)
                    filtered_df = filtered_df[
                        filtered_df[col].str.contains(val, case=False, na=False)
                    ]

            # STRING CONDITION
            elif isinstance(cond_val, str):

                # Numeric condition
                if any(cond_val.startswith(op) for op in ['>=', '<=', '==', '>', '<']):
                    
                    operator = cond_val[:2] if cond_val[:2] in ['>=', '<=', '=='] else cond_val[0]
                    value = float(cond_val[2:] if operator in ['>=', '<=', '=='] else cond_val[1:])

                    if operator == '>=':
                        filtered_df = filtered_df[filtered_df[col] >= value]
                    elif operator == '<=':
                        filtered_df = filtered_df[filtered_df[col] <= value]
                    elif operator == '>':
                        filtered_df = filtered_df[filtered_df[col] > value]
                    elif operator == '<':
                        filtered_df = filtered_df[filtered_df[col] < value]
                    elif operator in ['=', '==']:
                        filtered_df = filtered_df[filtered_df[col] == value]

                else:
                    # String contains
                    filtered_df[col] = filtered_df[col].astype(str)
                    filtered_df = filtered_df[
                        filtered_df[col].str.contains(cond_val, case=False, na=False)
                    ]

            # EXACT MATCH
            else:
                filtered_df = filtered_df[filtered_df[col] == cond_val]

    # -------------------------------
    # Drop NaNs in metric
    # -------------------------------
    filtered_df = filtered_df.dropna(subset=[metric])

    # -------------------------------
    # Sort results
    # -------------------------------
    filtered_df = filtered_df.sort_values(by=metric, ascending=not top)

    return filtered_df.head(n)


# -------------------------------
# Predefined KPI Queries (Wrappers)
# -------------------------------

def top_revenue(df, n=10):
    return rank_movies(df, "revenue_musd", True, n)


def top_budget(df, n=10):
    return rank_movies(df, "budget_musd", True, n)


def highest_profit(df, n=10):
    return rank_movies(df, "profit_musd", True, n)


def lowest_profit(df, n=10):
    return rank_movies(df, "profit_musd", False, n)


def highest_roi(df, n=10):
    return rank_movies(df, "roi", True, n, {"budget_musd": ">=10"})


def lowest_roi(df, n=10):
    return rank_movies(df, "roi", False, n, {"budget_musd": ">=10"})


def most_voted(df, n=10):
    return rank_movies(df, "vote_count", True, n)


def highest_rated(df, n=10):
    return rank_movies(df, "vote_average", True, n, {"vote_count": ">=10"})


def lowest_rated(df, n=10):
    return rank_movies(df, "vote_average", False, n, {"vote_count": ">=10"})


def most_popular(df, n=10):
    return rank_movies(df, "popularity", True, n)


# -------------------------------
# Advanced Search Queries
# -------------------------------

def search_scifi_action_bruce_willis(df):
    return rank_movies(
        df,
        metric="vote_average",
        top=True,
        n=10,
        filter_dict={
            "genres": ["Science Fiction", "Action"],
            "cast": ["Bruce Willis"]
        }
    )


def search_tarantino_uma(df):
    return rank_movies(
        df,
        metric="runtime",
        top=False,
        n=10,
        filter_dict={
            "cast": ["Uma Thurman"],
            "director": ["Quentin Tarantino"]
        }
    )


# -------------------------------
# Franchise vs Standalone Analysis
# -------------------------------

def franchise_vs_standalone(df):
    df = df.copy()

    df["is_franchise"] = df["belongs_to_collection"] != "Unknown"

    result = df.groupby("is_franchise").agg({
        "revenue_musd": "mean",
        "roi": "median",
        "budget_musd": "mean",
        "popularity": "mean",
        "vote_average": "mean"
    })

    result.index = ["Standalone", "Franchise"]

    return result


# -------------------------------
# Top Franchises
# -------------------------------

def top_franchises(df, n=10):
    data = df[df["belongs_to_collection"] != "Unknown"]

    result = data.groupby("belongs_to_collection").agg({
        "id": "count",
        "budget_musd": ["sum", "mean"],
        "revenue_musd": ["sum", "mean"],
        "vote_average": "mean"
    })

    result.columns = [
        "movie_count", "total_budget", "mean_budget",
        "total_revenue", "mean_revenue", "mean_rating"
    ]

    return result.sort_values(by="total_revenue", ascending=False).head(n)


# -------------------------------
# Top Directors
# -------------------------------

def top_directors(df, n=10):
    data = df[df["director"] != "Unknown"]

    result = data.groupby("director").agg({
        "id": "count",
        "revenue_musd": "sum",
        "vote_average": "mean"
    })

    result.columns = ["movie_count", "total_revenue", "mean_rating"]

    return result.sort_values(by="total_revenue", ascending=False).head(n)