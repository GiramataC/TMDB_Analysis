import asyncio
import aiohttp
import pandas as pd
import os
import sys
import time
import logging
from dotenv import load_dotenv
from pydantic import ValidationError
from config import MOVIE_IDS, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_FACTOR, SLEEP_BETWEEN_CALLS, RATE_LIMITING, OPERATION_TIMEOUT, SESSION_CONFIG
from rate_limiter import AsyncRateLimiter, CircuitBreaker, exponential_backoff, RateLimitMonitor
from validators import validate_api_response, MovieAPIResponse

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

# Validate API Key
if not API_KEY:
    logging.critical(
        "API_KEY environment variable not found. "
        "Please set API_KEY in your .env file or as an environment variable."
    )
    sys.exit(1)

logging.info(f"API Key loaded successfully (last 4 chars: ...{API_KEY[-4:]})") 

BASE_URL = "https://api.themoviedb.org/3/movie"

# Global rate limiting instances (initialized in main())
rate_limiter = None
circuit_breaker = None
rate_monitor = None

# Global retrieval statistics (populated by main_async)
retrieval_stats = {
    "total_requested": 0,
    "total_successful": 0,
    "total_failed": 0,
    "success_rate": 0,
    "rate": 0,
    "elapsed_time": 0,
    "circuit_breaks": 0,
}


# -------------------------------
# Helper: Async Request with Strict Rate Limiting
# -------------------------------
async def fetch_with_retries_async(session, url):
    """
    Makes an async HTTP GET request with strict rate limiting, circuit breaker, and retry logic.
    
    Implements:
    - Token bucket rate limiting (4 requests/second for TMDB)
    - Circuit breaker to prevent cascading failures
    - Exponential backoff with jitter on failures
    - Retries up to MAX_RETRIES times
    - Immediate 404 handling (no retries)
    
    Args:
        session (aiohttp.ClientSession): Async HTTP session
        url (str): The full URL to request, including any query parameters.
    
    Returns:
        dict: Parsed JSON response if successful.
        None: If the movie was not found (404), circuit is open, or all retries failed.
    """
    global rate_limiter, circuit_breaker, rate_monitor
    
    # Check circuit breaker before attempting request
    if not circuit_breaker.should_attempt():
        logging.warning("Circuit breaker OPEN - rejecting request")
        rate_monitor.increment_blocked()
        rate_monitor.increment_circuit_break()
        return None
    
    # Acquire rate limit token (awaits if needed)
    await rate_limiter.acquire(tokens=1)
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                
                # Success
                if response.status == 200:
                    data = await response.json()
                    
                    # Validate API response schema
                    try:
                        validated = validate_api_response(data)
                        logging.debug(f"API response validated for movie {validated.id}")
                        circuit_breaker.record_success()
                        rate_monitor.increment_sent()
                        # Return as dict for compatibility with existing code
                        return data
                    except ValidationError as e:
                        logging.error(f"API response validation failed: {e}")
                        circuit_breaker.record_failure()
                        if attempt < MAX_RETRIES:
                            wait_time = exponential_backoff(
                                attempt,
                                base=RATE_LIMITING["initial_backoff"],
                                max_backoff=RATE_LIMITING["max_backoff"]
                            )
                            logging.info(f"Retrying after validation error in {wait_time:.2f}s...")
                            await asyncio.sleep(wait_time)
                        continue
                
                # Not found - don't retry
                elif response.status == 404:
                    logging.warning(f"Movie not found (404): {url}")
                    circuit_breaker.record_success()
                    return None
                
                # Rate limit hit (429) - record failure
                elif response.status == 429:
                    logging.warning(f"Rate limit exceeded (429). Attempt {attempt}/{MAX_RETRIES}")
                    circuit_breaker.record_failure()
                    
                    if attempt < MAX_RETRIES:
                        wait_time = exponential_backoff(
                            attempt,
                            base=RATE_LIMITING["initial_backoff"],
                            max_backoff=RATE_LIMITING["max_backoff"]
                        )
                        logging.info(f"Backing off for {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
                
                # Other server errors
                else:
                    logging.warning(f"Attempt {attempt}/{MAX_RETRIES}: Status {response.status}")
                    circuit_breaker.record_failure()
                    
                    if attempt < MAX_RETRIES:
                        wait_time = exponential_backoff(
                            attempt,
                            base=RATE_LIMITING["initial_backoff"],
                            max_backoff=RATE_LIMITING["max_backoff"]
                        )
                        logging.info(f"Retrying in {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
        
        except asyncio.TimeoutError as e:
            logging.error(f"Request timeout (Attempt {attempt}/{MAX_RETRIES}): {e}")
            circuit_breaker.record_failure()
            
            if attempt < MAX_RETRIES:
                wait_time = exponential_backoff(
                    attempt,
                    base=RATE_LIMITING["initial_backoff"],
                    max_backoff=RATE_LIMITING["max_backoff"]
                )
                logging.info(f"Retrying in {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)
        
        except aiohttp.ClientError as e:
            logging.error(f"Request failed (Attempt {attempt}/{MAX_RETRIES}): {e}")
            circuit_breaker.record_failure()
            
            if attempt < MAX_RETRIES:
                wait_time = exponential_backoff(
                    attempt,
                    base=RATE_LIMITING["initial_backoff"],
                    max_backoff=RATE_LIMITING["max_backoff"]
                )
                logging.info(f"Retrying in {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)
    
    logging.error(f"Failed after {MAX_RETRIES} retries: {url}")
    return None


# -------------------------------
# Extract Movie + Credits (Async)
# -------------------------------
async def process_movie_async(session, movie_id):
    """
    Async fetch and process movie details and credits from the TMDB API.

    Makes a single async API call using `append_to_response=credits` to retrieve
    both movie metadata and credits in one request. Extracts the top 5 cast
    members and the director, flattens them into the main data dictionary,
    and removes the nested credits object before returning.

    Args:
        session (aiohttp.ClientSession): Async HTTP session
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
        data = await fetch_with_retries_async(session, url)
        
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
# Main Pipeline with Async Concurrency
# -------------------------------
async def main_async():
    """
    Run the movie data pipeline with strict rate limiting and async concurrency.

    Initializes global rate limiting, circuit breaker, and monitoring instances.
    Creates up to 4 concurrent workers to fetch and process movie data.
    Saves results to Parquet format in data/raw/.
    Tracks failed movies separately for audit and retry purposes.
    
    Rate Limiting & Concurrency Strategy:
    - 4 concurrent workers (semaphore)
    - Async token bucket: 4 requests/second (TMDB limit)
    - Circuit breaker: Opens after 5 failures, retries after 60s
    - Exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s (capped)
    - Monitoring: Tracks requests, blocks, and circuit breaks
    - Failed movie tracking: Logs and saves failed IDs for manual review

    Outputs:
        - data/raw/movies.parquet           (successful movies)
        - data/raw/failed_movies.csv        (failed movie IDs for retry)
    """
    global rate_limiter, circuit_breaker, rate_monitor
    
    # Initialize rate limiting components
    rate_limiter = AsyncRateLimiter(
        requests_per_second=RATE_LIMITING["requests_per_second"]
    )
    circuit_breaker = CircuitBreaker(
        failure_threshold=RATE_LIMITING["circuit_breaker_threshold"],
        timeout=RATE_LIMITING["circuit_breaker_timeout"]
    )
    rate_monitor = RateLimitMonitor()
    
    logging.info(
        f"Async Rate Limiting initialized: "
        f"{RATE_LIMITING['requests_per_second']} req/s | "
        f"Concurrency: 4 workers | "
        f"Circuit breaker: {RATE_LIMITING['circuit_breaker_threshold']} failures, "
        f"{RATE_LIMITING['circuit_breaker_timeout']}s timeout"
    )
    
    logging.info("Starting movie data fetching (async with 4 concurrent workers)...")
    
    # Filter valid movie IDs
    valid_ids = []
    invalid_ids = []
    for movie_id in MOVIE_IDS:
        if isinstance(movie_id, int) and movie_id > 0:
            valid_ids.append(movie_id)
        else:
            invalid_ids.append(movie_id)
            logging.warning(f"Skipping invalid movie_id: {movie_id}")
    
    # Create semaphore to limit concurrency to 4 workers
    semaphore = asyncio.Semaphore(4)
    
    async def fetch_with_semaphore(session, movie_id):
        """Wrapper to enforce concurrency limit"""
        async with semaphore:
            return await process_movie_async(session, movie_id)
    
    # Create aiohttp session with production-grade configuration
    connector = aiohttp.TCPConnector(
        limit=SESSION_CONFIG["connector_limit"],
        limit_per_host=SESSION_CONFIG["connector_limit_per_host"],
        ttl_dns_cache=SESSION_CONFIG["ttl_dns_cache"],
        enable_cleanup_closed=SESSION_CONFIG["enable_cleanup_closed"]
    )
    timeout = aiohttp.ClientTimeout(
        total=SESSION_CONFIG["timeout_total"],
        connect=SESSION_CONFIG["timeout_connect"],
        sock_read=SESSION_CONFIG["timeout_sock_read"],
        sock_connect=SESSION_CONFIG["timeout_sock_connect"]
    )
    
    logging.info(
        f"aiohttp Session configured: "
        f"Connector={SESSION_CONFIG['connector_limit']} total, "
        f"{SESSION_CONFIG['connector_limit_per_host']} per-host | "
        f"Timeout={SESSION_CONFIG['timeout_total']}s total, "
        f"{SESSION_CONFIG['timeout_sock_read']}s read"
    )
    
    # Fetch all movies concurrently with optimized session
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        connector_owner=True
    ) as session:
        tasks = [fetch_with_semaphore(session, mid) for mid in valid_ids]
        results = await asyncio.gather(*tasks, return_exceptions=False)
    
    # Separate successful and failed movies
    movies = []
    failed_ids = []
    
    for idx, result in enumerate(results):
        if result is not None:
            movies.append(result)
        else:
            failed_ids.append(valid_ids[idx])
    
    df = pd.DataFrame(movies)
    
    # Log summary statistics
    total_requested = len(valid_ids)
    total_successful = len(movies)
    total_failed = len(failed_ids)
    success_rate = (total_successful / total_requested * 100) if total_requested > 0 else 0
    
    logging.info(
        f"Fetching finished. "
        f"Requested: {total_requested} | "
        f"Successful: {total_successful} | "
        f"Failed: {total_failed} | "
        f"Success Rate: {success_rate:.1f}%"
    )
    
    if invalid_ids:
        logging.warning(f"Skipped {len(invalid_ids)} invalid movie IDs")
    
    if failed_ids:
        logging.warning(
            f"Failed to fetch {total_failed} movies: {failed_ids}. "
            f"See data/raw/failed_movies.csv for details."
        )
    
    # Log rate limiting statistics
    rate_monitor.log_stats()
    
    # Populate global retrieval stats for report generation
    global retrieval_stats
    elapsed_time = time.time() - rate_monitor.start_time
    retrieval_stats.update({
        "total_requested": total_requested,
        "total_successful": total_successful,
        "total_failed": total_failed,
        "success_rate": success_rate,
        "rate": round(rate_monitor.requests_sent / elapsed_time, 2) if elapsed_time > 0 else 0,
        "elapsed_time": round(elapsed_time, 1),
        "circuit_breaks": rate_monitor.circuit_breaks,
    })
    
    # Save results
    os.makedirs("data/raw", exist_ok=True)
    
    # Save successful movies
    df.to_parquet("data/raw/movies.parquet", index=False)
    logging.info(f"Saved {total_successful} successful movies to data/raw/")
    
    # Save failed movies for audit and retry
    if failed_ids:
        df_failed = pd.DataFrame({
            "movie_id": failed_ids,
            "status": "failed",
            "timestamp": pd.Timestamp.now(),
            "notes": "Failed to fetch from TMDB API. Check logs for details."
        })
        df_failed.to_csv("data/raw/failed_movies.csv", index=False)
        logging.info(f"Saved {total_failed} failed movie IDs to data/raw/failed_movies.csv")


async def main_async_with_timeout():
    """
    Wrapper around main_async() that enforces operation timeout.
    
    Uses asyncio.timeout() to ensure the entire pipeline completes within
    OPERATION_TIMEOUT seconds. This prevents indefinite hangs from network
    issues, slow servers, or other blocking conditions.
    """
    try:
        async with asyncio.timeout(OPERATION_TIMEOUT):
            await main_async()
    except asyncio.TimeoutError:
        logging.critical(
            f"Async pipeline task exceeded {OPERATION_TIMEOUT}s timeout. "
            "Cancelling all pending requests..."
        )
        raise


def main():
    """
    Synchronous entry point that runs the async main coroutine with timeout protection.
    
    Enforces a strict operation timeout to prevent runaway processes.
    If the pipeline exceeds OPERATION_TIMEOUT seconds, it will be forcefully terminated.
    
    Raises:
        asyncio.TimeoutError: If pipeline exceeds operation timeout
        SystemExit: On any unhandled exception (exit code 1)
    """
    try:
        asyncio.run(main_async_with_timeout())
    except asyncio.TimeoutError:
        logging.critical(
            f"Pipeline exceeded operation timeout of {OPERATION_TIMEOUT} seconds. "
            "This could indicate a hanging connection or extremely slow network."
        )
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Pipeline interrupted by user (Ctrl+C).")
        sys.exit(0)
    except Exception as e:
        logging.exception(f"Pipeline failed with unexpected error: {e}")
        sys.exit(1)


def get_retrieval_stats():
    """Return the last retrieval statistics collected"""
    return retrieval_stats.copy()


if __name__ == "__main__":
    main()