import time
import threading


class TokenBucketRateLimiter:
    def __init__(self, rate, burst):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            time.sleep(wait)


# getPricing limits: 0.5 RPS, burst 1 → 1 request every 2 seconds
pricing_limiter = TokenBucketRateLimiter(rate=0.5, burst=1)

# Fees API: 1 RPS, burst 2
fees_limiter = TokenBucketRateLimiter(rate=1.0, burst=2)
