import matplotlib.pyplot as plt
import pandas as pd

# Global style
plt.rcParams["figure.figsize"] = (8, 5)


# -------------------------------
# 1. Revenue vs Budget
# -------------------------------

def plot_revenue_vs_budget(df):
    plt.figure()
    plt.scatter(df["budget_musd"], df["revenue_musd"])
    
    plt.xlabel("Budget (Million USD)")
    plt.ylabel("Revenue (Million USD)")
    plt.title("Revenue vs Budget")
    
    plt.show()


# -------------------------------
# 2. ROI Distribution by Genre
# -------------------------------


def plot_roi_by_genre(df):
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

    plt.figure()
    genre_df.boxplot(column="roi", by="genres", rot=90)

    plt.title("ROI Distribution by Genre")
    plt.suptitle("")
    plt.xlabel("Genre")
    plt.ylabel("ROI")

    plt.tight_layout()
    plt.show()


# -------------------------------
# 3. Popularity vs Rating
# -------------------------------

def plot_popularity_vs_rating(df):
    plt.figure()
    plt.scatter(df["popularity"], df["vote_average"])

    plt.xlabel("Popularity")
    plt.ylabel("Rating")
    plt.title("Popularity vs Rating")

    plt.show()


# -------------------------------
# 4. Yearly Revenue Trend
# -------------------------------

def plot_yearly_revenue(df):
    df = df.copy()

    df["year"] = df["release_date"].dt.year

    yearly = df.groupby("year")["revenue_musd"].sum()

    plt.figure()
    yearly.plot()

    plt.xlabel("Year")
    plt.ylabel("Total Revenue (Million USD)")
    plt.title("Yearly Box Office Trends")

    plt.show()


# -------------------------------
# 5. Franchise vs Standalone Comparison
# -------------------------------

def plot_franchise_vs_standalone(df):
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

    plt.show()