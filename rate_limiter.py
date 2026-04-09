import time
import random
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# -------------------------------
# Token Bucket Rate Limiter
# -------------------------------
class RateLimiter:
    """
    Token bucket rate limiter to enforce strict API rate limits.
    
    Maintains a bucket of tokens that refill at a constant rate.
    Each request costs 1 token. If tokens unavailable, waits until
    tokens are refilled.
    
    Example:
        limiter = RateLimiter(requests_per_second=4)
        limiter.acquire()  # Wait if needed, then consume 1 token
    """
    
    def __init__(self, requests_per_second=4):
        """
        Args:
            requests_per_second (float): Refill rate (e.g., 4 = 4 requests/second)
        """
        self.capacity = requests_per_second
        self.tokens = requests_per_second
        self.refill_rate = requests_per_second
        self.last_refill = time.time()
        self.lock_time = 0
    
    def acquire(self, tokens=1):
        """
        Acquire tokens from bucket. Blocks until available.
        
        Args:
            tokens (int): Number of tokens to acquire (default: 1)
        """
        while self.tokens < tokens:
            self._refill()
            if self.tokens < tokens:
                time.sleep(0.01)  # Small sleep to avoid busy waiting
        
        self.tokens -= tokens
    
    def _refill(self):
        """Refill tokens based on elapsed time since last refill."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(
            self.capacity,
            self.tokens + elapsed * self.refill_rate
        )
        self.last_refill = now


# -------------------------------
# Circuit Breaker Pattern
# -------------------------------
class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.
    
    States:
    - CLOSED: Normal operation, requests go through
    - OPEN: Too many failures, reject all requests
    - HALF_OPEN: Testing if service recovered, allow 1 request
    
    Example:
        breaker = CircuitBreaker(failure_threshold=5, timeout=60)
        if breaker.should_attempt():
            try:
                # make request
                breaker.record_success()
            except Exception:
                breaker.record_failure()
    """
    
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
    
    def __init__(self, failure_threshold=5, timeout=60):
        """
        Args:
            failure_threshold (int): Failures before opening circuit
            timeout (int): Seconds before attempting recovery
        """
        self.state = self.CLOSED
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.open_time = None
        self.success_count = 0
        self.logger = logging.getLogger(__name__)
    
    def should_attempt(self):
        """
        Check if request should be attempted.
        
        Returns:
            bool: True if circuit allows request, False otherwise
        """
        if self.state == self.CLOSED:
            return True
        
        if self.state == self.OPEN:
            elapsed = time.time() - self.open_time
            if elapsed > self.timeout:
                self.logger.info(f"Circuit breaker: OPEN → HALF_OPEN (timeout={self.timeout}s)")
                self.state = self.HALF_OPEN
                return True
            return False
        
        # HALF_OPEN state
        return True
    
    def record_success(self):
        """Record successful request."""
        if self.state == self.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= 2:  # 2 successful requests to close
                self.logger.info("Circuit breaker: HALF_OPEN → CLOSED")
                self.state = self.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == self.CLOSED:
            self.failure_count = 0
    
    def record_failure(self):
        """Record failed request."""
        self.failure_count += 1
        self.success_count = 0
        
        if self.failure_count >= self.failure_threshold:
            self.logger.warning(
                f"Circuit breaker: CLOSED → OPEN (failures={self.failure_count}/{self.failure_threshold})"
            )
            self.state = self.OPEN
            self.open_time = time.time()


# -------------------------------
# Exponential Backoff with Jitter
# -------------------------------
def exponential_backoff(attempt, base=2, max_backoff=32, jitter=True):
    """
    Calculate exponential backoff wait time.
    
    Backoff sequence: 1s, 2s, 4s, 8s, 16s, 32s (capped)
    Jitter: ±10% random variation to avoid thundering herd
    
    Args:
        attempt (int): Attempt number (1-indexed)
        base (int): Base for exponentiation (default: 2)
        max_backoff (int): Cap on backoff time in seconds (default: 32)
        jitter (bool): Add random jitter (default: True)
    
    Returns:
        float: Wait time in seconds
    """
    wait_time = min(base ** attempt, max_backoff)
    
    if jitter:
        jitter_amount = wait_time * random.uniform(0, 0.1)
        wait_time += jitter_amount
    
    return wait_time


# -------------------------------
# Rate Limit Monitor
# -------------------------------
class RateLimitMonitor:
    """
    Monitor and log rate limiting statistics.
    
    Tracks:
    - Total requests made
    - Requests blocked or delayed
    - Circuit breaker activations
    - Effective request rate
    
    Example:
        monitor = RateLimitMonitor()
        monitor.increment_sent()
        monitor.log_stats()
    """
    
    def __init__(self):
        self.requests_sent = 0
        self.requests_blocked = 0
        self.circuit_breaks = 0
        self.start_time = time.time()
        self.logger = logging.getLogger(__name__)
    
    def increment_sent(self):
        """Record a successful API request."""
        self.requests_sent += 1
    
    def increment_blocked(self):
        """Record a blocked request due to rate limiting."""
        self.requests_blocked += 1
    
    def increment_circuit_break(self):
        """Record a circuit breaker rejection."""
        self.circuit_breaks += 1
    
    def log_stats(self):
        """Log aggregated statistics."""
        elapsed = time.time() - self.start_time
        
        if elapsed > 0:
            actual_rate = self.requests_sent / elapsed
        else:
            actual_rate = 0
        
        block_rate = (
            (self.requests_blocked / self.requests_sent * 100)
            if self.requests_sent > 0
            else 0
        )
        
        self.logger.info(
            f"Rate Limit Stats | Requests: {self.requests_sent} | "
            f"Rate: {actual_rate:.2f} req/s | Blocked: {block_rate:.1f}% | "
            f"Circuit breaks: {self.circuit_breaks} | Elapsed: {elapsed:.1f}s"
        )


# -------------------------------
# Async Token Bucket Rate Limiter
# -------------------------------
class AsyncRateLimiter:
    """
    Async-compatible token bucket rate limiter for asyncio tasks.
    
    Maintains tokens that refill at a constant rate. Each async request
    must acquire a token before proceeding. If no tokens available, awaits
    until tokens are refilled.
    
    Example:
        limiter = AsyncRateLimiter(requests_per_second=4)
        await limiter.acquire()  # Wait if needed, then consume 1 token
    """
    
    def __init__(self, requests_per_second=4):
        """
        Args:
            requests_per_second (float): Refill rate
        """
        import asyncio
        self.capacity = requests_per_second
        self.tokens = requests_per_second
        self.refill_rate = requests_per_second
        self.last_refill = time.time()
        self.lock = asyncio.Lock()
    
    async def acquire(self, tokens=1):
        """
        Acquire tokens from bucket. Awaits until available.
        
        Args:
            tokens (int): Number of tokens to acquire (default: 1)
        """
        async with self.lock:
            while self.tokens < tokens:
                await self._wait_and_refill()
            self.tokens -= tokens
    
    async def _wait_and_refill(self):
        """Wait and refill tokens based on elapsed time."""
        import asyncio
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(
            self.capacity,
            self.tokens + elapsed * self.refill_rate
        )
        self.last_refill = now
        
        if self.tokens < 1:
            # Calculate wait time for next refill
            wait_time = (1 - self.tokens) / self.refill_rate
            await asyncio.sleep(wait_time)
