from asyncio import iscoroutinefunction, run
from functools import wraps
from hashlib import md5
from inspect import ismethod
from threading import Lock
from typing import Callable, Optional, cast

from dill import dumps
from loguru import logger

from scriptman.core.config import config
from scriptman.powers.cache._backend import CacheBackend
from scriptman.powers.generics import AsyncFunc, SyncFunc, T

try:
    from scriptman.powers.cache.diskcache import FanoutCacheBackend
except ImportError:
    raise ImportError(
        "DiskCache backend is not installed. "
        "Please install it with `pip install scriptman[cache]`"
    )


class CacheManager:
    """Thread-safe singleton cache manager with safe backend switching capabilities"""

    DEFAULT_BACKEND: type[CacheBackend] = FanoutCacheBackend
    _instance = None
    _lock = Lock()
    _backend_switch_lock = Lock()
    _active_operations = 0
    _active_operations_lock = Lock()

    def __new__(cls, backend: Optional[CacheBackend] = None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._backend = backend or cls.DEFAULT_BACKEND()
            return cls._instance

    def __init__(self, backend: Optional[CacheBackend] = None):
        with self._lock:
            if not hasattr(self, "_initialized"):
                self._backend = backend or self.DEFAULT_BACKEND()
                self._initialized = True

    @property
    def backend(self) -> CacheBackend:
        return self._backend

    def switch_backend(
        self, new_backend: type[CacheBackend], *backend_args, **backend_kwargs
    ) -> bool:
        """
        Safely switch to a new backend after all current operations complete.

        Args:
            new_backend: The new cache backend to switch to
            *backend_args: Positional arguments to pass to the new backend
            **backend_kwargs: Keyword arguments to pass to the new backend

        Returns:
            bool: True if switch was successful, False otherwise
        """
        # Save the old backend for reference
        old_backend = self._backend

        try:
            # First get the backend switch lock
            if not self._backend_switch_lock.acquire(timeout=30):
                logger.error("❌ Could not acquire backend switch lock")
                return False

            # Wait for all active operations to complete
            while True:
                with self._active_operations_lock:
                    if self._active_operations == 0:
                        # No active operations, safe to switch
                        self._backend = new_backend(*backend_args, **backend_kwargs)
                        logger.info(
                            "✅ Successfully switched backend "
                            f"from {type(old_backend).__name__} "
                            f"to {type(new_backend).__name__}"
                        )
                        return True

                # Still have active operations, wait a bit
                from time import sleep

                sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Error switching backend: {e}")
            self._backend = old_backend
            return False

        finally:
            self._backend_switch_lock.release()

    @classmethod
    def get_instance(cls) -> "CacheManager":
        """Get the singleton instance of CacheManager"""
        return cls()

    def _track_operation(self):
        """Context manager to track active cache operations"""

        class OperationTracker:
            def __init__(self, cache_manager):
                self.cache_manager = cache_manager

            def __enter__(self):
                with self.cache_manager._active_operations_lock:
                    self.cache_manager._active_operations += 1

            def __exit__(self, exc_type, exc_val, exc_tb):
                with self.cache_manager._active_operations_lock:
                    self.cache_manager._active_operations -= 1

        return OperationTracker(self)

    @staticmethod
    def cache_result(
        ttl: Optional[int] = config.env.get("CACHE.TTL"), **kwargs
    ) -> Callable[..., SyncFunc[T] | AsyncFunc[T]]:
        """
        📦 Decorator for caching function results. Works with both synchronous and
        asynchronous functions.

        NOTE: This only works for JSON Serializable arguments and returns.

        Args:
            ttl (Optional[int]): The number of seconds until the key expires.
                Defaults to None.
            **kwargs: Additional keyword arguments to be passed to the cache backend's
                set method when storing values.

        Returns:
            Callable[..., SyncFunc[T] | AsyncFunc[T]]: Decorated function.
        """
        cache_manager = CacheManager.get_instance()

        def decorator(func: SyncFunc[T] | AsyncFunc[T]) -> SyncFunc[T] | AsyncFunc[T]:
            @wraps(func)
            async def async_wrapper(*f_args, **f_kwargs) -> T:
                key = CacheManager.generate_callable_key(func, f_args, f_kwargs)

                with cache_manager._track_operation():
                    if result := cache_manager.backend.get(key=key):
                        logger.success(f"✅ Cache hit for key: {key}")
                        return result

                    logger.warning(f"❔ Cache miss for key: {key}")

                    if iscoroutinefunction(func):
                        result = await cast(AsyncFunc[T], func)(*f_args, **f_kwargs)
                    else:
                        result = cast(SyncFunc[T], func)(*f_args, **f_kwargs)

                    if cache_manager.backend.set(
                        key=key, value=result, ttl=ttl, **kwargs
                    ):
                        logger.info(f"✅ Stored result in cache with key: {key}")
                    else:
                        logger.error(
                            f"❌ Failed to store result in cache with key: {key}"
                        )

                    return result

            @wraps(func)
            def sync_wrapper(*args, **kwargs) -> T:
                return run(async_wrapper(*args, **kwargs))

            return async_wrapper if iscoroutinefunction(func) else sync_wrapper

        return decorator

    @staticmethod
    def generate_callable_key(func: Callable, args: tuple, kwargs: dict) -> str:
        """Generate a unique cache key for the function and arguments."""
        key_properties = {
            "func": CacheManager.get_function_name(func),
            "kwargs": CacheManager.sort_dictionary(kwargs),
            "args": CacheManager.remove_self_or_cls_from_args(func, args),
        }
        return md5(dumps(key_properties)).hexdigest()

    @staticmethod
    def sort_dictionary(
        dictionary: dict, key: Optional[Callable] = None, reverse: bool = False
    ) -> dict:
        return dict(sorted(dictionary.items(), key=key, reverse=reverse))

    @staticmethod
    def remove_self_or_cls_from_args(func: Callable, args: tuple) -> tuple:
        return (
            args[1:] if args and ismethod(getattr(args[0], func.__name__, None)) else args
        )

    @staticmethod
    def get_function_name(func: Callable) -> str:
        module = getattr(func, "__module__", "<unknown_module>")
        name = getattr(func, "__qualname__", type(func).__name__)
        return f"{module}.{name}"
