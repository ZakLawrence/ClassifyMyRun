def nearest(items,pivot):
    return min(items, key=lambda x: abs(x-pivot))

def chunk_list(lst:list, chunk_size:int):
    for i in range(0,len(lst),chunk_size):
        yield lst[i:i+chunk_size]

class RateLimitExceeded(Exception):
    """Raised when the Strava API rate limit is reached."""
    def __init__(self, scope: str, used: int, limit: int):
        super().__init__(f"Rate limit exceeded for {scope}: {used}/{limit}")
        self.scope = scope
        self.used = used
        self.limit = limit