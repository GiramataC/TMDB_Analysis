import requests
import pandas as pd
import os
import time
import logging
from dotenv import load_dotenv
from config import MOVIE_IDS, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_FACTOR, SLEEP_BETWEEN_CALLS

# -------------------------------
# Setup
# -------------------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "movie_pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

BASE_URL = "https://api.themoviedb.org/3/movie"


# -------------------------------
# Helper: Request with Retries
# -------------------------------
def fetch_with_retries(url):
    """
    Makes an HTTP GET request to the given URL with retry logic.
    
    On a successful response (200), returns the parsed JSON data.
    On a 404, logs a warning and returns None immediately without retrying.
    On any other failure (non-200 status or request exception), retries up
    to MAX_RETRIES times, waiting BACKOFF_FACTOR ** attempt seconds between
    each retry. Returns None if all attempts are exhausted.
    
    Args:
        url (str): The full URL to request, including any query parameters.
    
    Returns:
        dict: Parsed JSON response if successful.
        None: If the movie was not found (404) or all retries failed.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                return response.json()
            
            elif response.status_code == 404:
                logging.warning(f"Movie not found (404): {url}")
                return None
            
            else:
                logging.warning(f"Attempt {attempt}: Status {response.status_code}")
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {attempt} failed: {e}")
        
        sleep_time = BACKOFF_FACTOR ** attempt
        logging.info(f"Retrying in {sleep_time:.2f}s...")
        time.sleep(sleep_time)
    
    logging.error(f"Failed after {MAX_RETRIES} retries: {url}")
    return None


# -------------------------------
# Extract Movie + Credits
# -------------------------------
def process_movie(movie_id):
    """
    Fetch and process movie details and credits from the TMDB API.

    Makes a single API call using `append_to_response=credits` to retrieve
    both movie metadata and credits in one request. Extracts the top 5 cast
    members and the director, flattens them into the main data dictionary,
    and removes the nested credits object before returning.

    Args:
        movie_id (int): The TMDB movie ID to fetch.

    Returns:
        dict: A flat dictionary containing all movie fields from the API
              response, plus the following derived fields:
                - cast (str): Pipe-separated names of up to 5 cast members.
                - cast_size (int): Total number of cast members.
                - director (str or None): Name of the director, or None if not found.
                - crew_size (int): Total number of crew members.
        None: If the API returns no data or an unexpected error occurs.
    """
   
    try:
        url = f"{BASE_URL}/{movie_id}?api_key={API_KEY}&append_to_response=credits"
        data = fetch_with_retries(url)
        
        if data is None:
            return None
        
        # -------------------------------
        # Extract credits
        # -------------------------------
        credits = data.get("credits", {})
        cast_list = credits.get("cast", [])
        crew_list = credits.get("crew", [])
        
        cast = [actor.get("name") for actor in cast_list[:5] if "name" in actor]
        
        director = next(
            (member.get("name") for member in crew_list if member.get("job") == "Director"),
            None
        )
        
        # -------------------------------
        # Add new fields to FULL data
        # -------------------------------
        data["cast"] = "|".join(cast)
        data["cast_size"] = len(cast_list)
        data["director"] = director
        data["crew_size"] = len(crew_list)
        
        # remove nested credits to avoid messy DataFrame
        data.pop("credits", None)
        
        return data
    
    except Exception as e:
        logging.exception(f"Unexpected error processing movie_id={movie_id}: {e}")
        return None


# -------------------------------
# Main Pipeline
# -------------------------------
def main():
    """
    Run the movie data pipeline from fetching to saving.

    Iterates over the list of movie IDs defined in config.py, validates each ID,
    and calls process_movie() to fetch and process the data. Skips invalid or
    failed entries with a warning. Once all movies are processed, saves the
    collected data to both CSV and Parquet formats in data/raw/.

    Outputs:
        - data/raw/movies_output.csv
        - data/raw/movies.parquet
    """
    movies = []
    
    logging.info("Starting movie data fetching...")
    
    for movie_id in MOVIE_IDS:
        if not isinstance(movie_id, int) or movie_id <= 0:
            logging.warning(f"Skipping invalid movie_id: {movie_id}")
            continue
        
        movie_data = process_movie(movie_id)
        
        if movie_data:
            movies.append(movie_data)
            logging.info(f"Processed: {movie_data['title']}")
        else:
            logging.warning(f"Failed to process movie_id={movie_id}")
        
        time.sleep(SLEEP_BETWEEN_CALLS)
    
    df = pd.DataFrame(movies)
    
    logging.info(f"Fetching finished. Total movies: {len(df)}")
    
    df.to_csv("data/raw/movies_output.csv", index=False)
    df.to_parquet("data/raw/movies.parquet", index=False)
    logging.info("Saved to data/raw/movies_output.csv")


if __name__ == "__main__":
    main()