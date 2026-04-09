MOVIE_IDS =  [0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513]
REQUEST_TIMEOUT = 5  # seconds
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.5
SLEEP_BETWEEN_CALLS = 0.25

# -------------------------------
# Strict Rate Limiting Configuration
# TMDB Limit: 40 requests / 10 seconds = 4 requests/second
# -------------------------------
RATE_LIMITING = {
    "requests_per_second": 4,           # 40/10s for TMDB API
    "max_retries": 5,                   # Increased from 3
    "initial_backoff": 1,               # seconds
    "max_backoff": 32,                  # cap exponential backoff
    "circuit_breaker_threshold": 5,     # failures before opening
    "circuit_breaker_timeout": 60,      # 1 minute before retry attempt
}

# -------------------------------
# Operation Timeout Configuration
# Maximum time allowed for entire pipeline to complete
# -------------------------------
OPERATION_TIMEOUT = 600  # 10 minutes max for entire pipeline

# -------------------------------
# aiohttp Session Configuration
# Optimized for reliable API communication
# -------------------------------
SESSION_CONFIG = {
    # Connector limits
    "connector_limit": 4,                # Max 4 concurrent connections (global)
    "connector_limit_per_host": 10,      # Max 10 per TMDB host (single domain)
    "ttl_dns_cache": 300,                # Cache DNS for 5 minutes
    "enable_cleanup_closed": True,       # Auto-cleanup closed connections
    
    # Timeout settings (in seconds)
    "timeout_total": 30,                 # Max 30s for entire request
    "timeout_connect": 10,               # Max 10s to establish connection
    "timeout_sock_read": 15,             # Max 15s to read response
    "timeout_sock_connect": 10,          # Max 10s for socket connection
}
