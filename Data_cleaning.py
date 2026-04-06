import pandas as pd
import numpy as np
import logging
import ast

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------------
# Helpers
# -------------------------------

def safe_extract_names(x):
    """Extract 'name' fields from list of dicts."""
    # if isinstance(x, list):
    #     return "| ".join([d.get("name", "") for d in x if isinstance(d, dict)])
    # return np.nan
    if isinstance(x, list) and len(x) > 0:
        names = []
        for d in x:
            if isinstance(d, dict) and "name" in d:
                names.append(d["name"])
        if names:
            return "|".join(names)
    return np.nan


def safe_extract_collection(x):
    """Extract collection name."""
    if isinstance(x, dict):
        return x.get("name", np.nan)
    return np.nan



def parse_and_extract(x):
    """Convert stringified list of dicts to list, then extract names."""
    
    # if not isinstance(x, (list, str)):
    #     return np.nan
    
    # # Convert string to list if necessary
    # if isinstance(x, str):
    #     try:
    #         x = ast.literal_eval(x)
    #     except Exception:
    #         return np.nan
    
    # # Now extract names
    # return safe_extract_names(x)

    """Convert stringified list of dicts to list, then extract names."""
    if x is None:
        return np.nan
    
    # Handle numpy arrays — convert to list first
    if isinstance(x, np.ndarray):
        x = x.tolist()
    
    # Handle strings — convert to list
    if isinstance(x, str):
        try:
            x = ast.literal_eval(x)
        except Exception:
            return np.nan
    
    # Now extract names from list
    if isinstance(x, list):
        return safe_extract_names(x)
    
    return np.nan

# -------------------------------
# Main Cleaning Function
# -------------------------------

def clean_movies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and preprocess movie dataset.
    Returns a cleaned DataFrame.
    """

    logging.info("Starting data cleaning...")
    df = df.copy()

    # -------------------------------
    # 1. Drop irrelevant columns
    # -------------------------------
    columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')

    # -------------------------------
    # 2. Flatten JSON-like columns
    # -------------------------------
    nested_cols = [
        'genres', 'production_countries',
        'production_companies', 'spoken_languages'
    ]

    for col in nested_cols:
        if col in df.columns:
            # df[col] = df[col].apply(safe_extract_names)
            df[col] = df[col].apply(parse_and_extract)

    if 'belongs_to_collection' in df.columns:
        df['belongs_to_collection'] = df['belongs_to_collection'].apply(safe_extract_collection)

    # -------------------------------
    # 3. Convert data types
    # -------------------------------
    numeric_cols = [
        "budget", "id", "popularity",
        "revenue", "runtime", "vote_average", "vote_count"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert release_date
    if "release_date" in df.columns:
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

    # -------------------------------
    # 4. Handle invalid / missing values
    # -------------------------------
    # Replace unrealistic zeros
    for col in ["budget", "revenue", "runtime"]:
        if col in df.columns:
            df[col] = df[col].replace(0, np.nan)

    # Replace placeholder text
    placeholders = ["No Data", "N/A", "None", "", "null"]
    for col in ["overview", "tagline"]:
        if col in df.columns:
            df[col] = df[col].replace(placeholders, np.nan)

    # Fix vote_average when no votes
    if "vote_count" in df.columns and "vote_average" in df.columns:
        df.loc[df["vote_count"] == 0, "vote_average"] = np.nan

    # -------------------------------
    # 5. Unit normalization
    # -------------------------------
    if "budget" in df.columns:
        df["budget_musd"] = df["budget"] / 1_000_000

    if "revenue" in df.columns:
        df["revenue_musd"] = df["revenue"] / 1_000_000

    # if "budget_musd" in df.columns and "revenue_musd" in df.columns:
    #     df["roi"] = (df["revenue_musd"] - df["budget_musd"]) / df["budget_musd"]

    # -------------------------------
    # 6. Fill categorical missing values
    # -------------------------------
    categorical_cols = df.select_dtypes(include="object").columns
    df[categorical_cols] = df[categorical_cols].fillna("Unknown")

    # -------------------------------
    # 7. Remove duplicates & invalid rows
    # -------------------------------
    if "id" in df.columns:
        df = df.drop_duplicates(subset="id")

    df = df.dropna(subset=["id", "title"], how="any")

    # Keep rows with sufficient data
    df = df.dropna(thresh=10)

    # -------------------------------
    # 8. Keep only released movies
    # -------------------------------
    if "status" in df.columns:
        df = df[df["status"] == "Released"]
        df = df.drop(columns=["status"])

    # -------------------------------
    # 9. Final column selection
    # -------------------------------
    final_columns = [
        'id', 'title', 'tagline', 'release_date', 'genres',
        'belongs_to_collection', 'original_language',
        'budget_musd', 'revenue_musd',
        'production_companies', 'production_countries',
        'vote_count', 'vote_average', 'popularity',
        'runtime', 'overview', 'spoken_languages',
        'poster_path', 'cast', 'cast_size',
        'director', 'crew_size'
    ]

    final_columns = [col for col in final_columns if col in df.columns]
    df = df[final_columns]

    # -------------------------------
    # 10. Reset index
    # -------------------------------
    df = df.reset_index(drop=True)

    logging.info(f"Cleaning completed. Final shape: {df.shape}")

    return df