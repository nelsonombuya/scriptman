from abc import ABC, abstractmethod
from typing import Any, Optional


class CacheBackend(ABC):
    """Abstract base class for cache implementations"""

    @property
    @abstractmethod
    def cache(self) -> Any:
        """The cache object used by the backend"""
        pass

    @abstractmethod
    def get(self, key: str, **kwargs: Any) -> Any:
        """Retrieve a value from cache"""
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None, **kwargs: Any) -> bool:
        """Store a value in cache"""
        pass

    @abstractmethod
    def delete(self, key: str, **kwargs: Any) -> bool:
        """Delete a value from cache"""
        pass
