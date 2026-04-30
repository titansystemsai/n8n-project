"""
Shared Brave Search API caller with global token-bucket rate limiter.

All step files import call_brave() from here so every Brave request across
all concurrent GPT agents goes through the same 1 req/sec bucket.

Brave's hard limit: 1 request/second (sliding window).
Burst of 3 allows the first few calls of a new lead to fire quickly, then
the bucket drains to a steady 1/sec cadence.

On 429: backs off and retries up to 3 times before returning empty results
(never raises — callers treat an empty result as "no search data").
"""
from __future__ import annotations

import logging
import os
import threading
import time

import httpx

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Token bucket — shared across ALL threads in the process
# ---------------------------------------------------------------------------

class _TokenBucket:
    def __init__(self, rate: float = 1.0, burst: int = 3):
        self._rate   = rate   # tokens replenished per second
        self._burst  = burst  # maximum tokens held at once
        self._tokens = float(burst)
        self._last   = time.monotonic()
        self._lock   = threading.Lock()

    def acquire(self) -> None:
        """Block until one token is available, then consume it."""
        while True:
            with self._lock:
                now     = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
                self._last   = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Calculate how long until next token is ready
                wait = (1.0 - self._tokens) / self._rate
            time.sleep(wait)


_bucket = _TokenBucket(rate=1.0, burst=3)


# ---------------------------------------------------------------------------
# Public caller
# ---------------------------------------------------------------------------

def call_brave(query: str, count: int = 5) -> list[dict]:
    """
    Search Brave and return snippet dicts. Never raises.

    Acquires a rate-limit token before each attempt, then retries up to 3×
    on 429 with exponential backoff. Returns [] on persistent failure so the
    GPT agent loop continues without crashing.
    """
    api_key = os.environ.get("BRAVE_API_KEY", "")

    for attempt in range(3):
        _bucket.acquire()  # always wait for a token, even on retry
        try:
            resp = httpx.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": query, "count": min(count, 10)},
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "X-Subscription-Token": api_key,
                },
                timeout=15,
            )

            if resp.status_code == 429:
                backoff = 2 ** attempt
                log.warning("Brave 429 on attempt %d — sleeping %ds", attempt + 1, backoff)
                time.sleep(backoff)
                continue

            resp.raise_for_status()
            return [
                {
                    "title":       r.get("title", ""),
                    "url":         r.get("url", ""),
                    "description": r.get("description", ""),
                }
                for r in resp.json().get("web", {}).get("results", [])
            ]

        except httpx.TimeoutException:
            log.warning("Brave timeout on attempt %d for query: %s", attempt + 1, query[:60])
        except httpx.HTTPStatusError as e:
            log.warning("Brave HTTP error %s on attempt %d", e.response.status_code, attempt + 1)
        except Exception as e:
            log.warning("Brave unexpected error on attempt %d: %s", attempt + 1, e)

    log.error("Brave failed after 3 attempts for query: %s", query[:80])
    return []
