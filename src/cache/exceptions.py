class CacheConnectionError(Exception):
    """Raised when there's an error connecting to the cache server."""
    pass


class CacheOperationError(Exception):
    """Raised when there's an error performing a cache operation."""
    pass


class CacheKeyError(Exception):
    """Raised when a requested key is not found in the cache."""
    pass
