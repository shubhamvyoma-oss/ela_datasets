"""
edmingle_rate_limiter.py

A rolling-window rate limiter — lifted directly from RollingRateLimiter in
edmingle_student_course_sync.py. Tracks call timestamps in a deque;
acquire() blocks until there's room for another call within the window.
This paces requests smoothly against max_calls_per_minute instead of a
flat --delay between calls, and can be reset() after a 429 cool-down so
the limiter doesn't immediately re-trigger a rate limit on resumption.
"""

import logging
import time
from collections import deque
from typing import Callable


class RollingRateLimiter:
    def __init__(
        self,
        max_calls: int,
        window_seconds: float,
        logger: logging.Logger,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self.logger = logger
        self.clock = clock
        self.sleep = sleep
        self.calls: deque[float] = deque()

    def acquire(self) -> None:
        while True:
            now = self.clock()
            while self.calls and now - self.calls[0] >= self.window_seconds:
                self.calls.popleft()
            if len(self.calls) < self.max_calls:
                self.calls.append(now)
                return
            wait_seconds = max(0.0, self.window_seconds - (now - self.calls[0]))
            if wait_seconds >= 1.0:
                self.logger.info(f"Rate limit reached; waiting {wait_seconds:.2f} seconds")
            else:
                self.logger.debug(f"Rate-limit pacing wait: {wait_seconds:.2f} seconds")
            self.sleep(wait_seconds)

    def reset(self) -> None:
        self.calls.clear()
