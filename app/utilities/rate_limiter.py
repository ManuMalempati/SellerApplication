import time, threading

class TokenBucketRateLimiter:
    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_update = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            # Across threads under same process we follow the rate limits
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                # We cannot exceed burst
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                # We know self.tokens has less than 1 token. e.g 0.3.
                # 1-0.3 = we still need 0.7 tokens. 0.7/self.rate. e.g. 0.7/0.5 gives how many seconds we need to wait
                wait_time = (1.0 - self.tokens) / self.rate

            time.sleep(wait_time)