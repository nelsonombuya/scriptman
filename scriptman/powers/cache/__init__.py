try:
    from functools import wraps
    from hashlib import md5
    from inspect import ismethod
    from threading import Lock
    from typing import Any, Callable, Generic, Optional, cast

    from dill import dumps
    from loguru import logger

    from scriptman.core.config import config
    from scriptman.powers.cache._backend import CacheBackend
    from scriptman.powers.cache.diskcache import FanoutCacheBackend
    from scriptman.powers.generics import AsyncFunc, SyncFunc, T
    from scriptman.powers.time_calculator import TimeCalculator
except ImportError:
    raise ImportError(
        "DiskCache backend is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[cache]"
    )


class CacheManager(Generic[T]):
    """Thread-safe singleton cache manager with safe backend switching capabilities"""

    _active_operations: int = 0
    _active_operations_lock: Lock = Lock()

    __lock: Lock = Lock()
    __backend: CacheBackend
    __initialized: bool = False
    __backend_switch_lock: Lock = Lock()
    __instance: Optional["CacheManager[T]"] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "CacheManager[T]":
        with cls.__lock:
            if cls.__instance is None:
                cls.__instance = super().__new__(cls, *args, **kwargs)
            return cls.__instance

    def __init__(self, backend: CacheBackend = FanoutCacheBackend()):
        if not self.__initialized:
            with self.__lock:
                self.__backend = backend
                self.__initialized = True

    @property
    def backend(self) -> CacheBackend:
        """⚙ Get the current cache backend used by the cache manager"""
        return self.__backend

    def switch_backend(
        self, new_backend: type[CacheBackend], *backend_args: Any, **backend_kwargs: Any
    ) -> bool:
        """
        🔁 Safely switch to a new backend after all current operations complete.

        Args:
            new_backend: The new cache backend to switch to
            *backend_args: Positional arguments to pass to the new backend
            **backend_kwargs: Keyword arguments to pass to the new backend

        Returns:
            bool: True if switch was successful, False otherwise
        """
        old_backend = self.__backend  # Save the old backend for reference
        try:
            # First get the backend switch lock
            if not self.__backend_switch_lock.acquire(timeout=30):
                logger.error("❌ Could not acquire backend switch lock")
                return False

            # Wait for all active operations to complete
            while True:
                with self._active_operations_lock:
                    if self._active_operations == 0:
                        # No active operations, safe to switch
                        self.__backend = new_backend(*backend_args, **backend_kwargs)
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
            self.__backend = old_backend
            return False

        finally:
            self.__backend_switch_lock.release()

    @classmethod
    def get_instance(cls, *args: Any, **kwargs: Any) -> "CacheManager[T]":
        """🚀 Get the singleton instance of CacheManager"""
        return cls(*args, **kwargs)

    def _track_operation(self) -> "OperationTracker[T]":
        """🔎 Context manager to track active cache operations"""
        return OperationTracker[T](self)

    @staticmethod
    def cache_result(
        ttl: Optional[int] = config.get("CACHE.TTL"), **backend_kwargs: Any
    ) -> Callable[[SyncFunc[T]], SyncFunc[T]]:
        """
        📦 Decorator for caching function results. Works with synchronous functions.

        NOTE: This only works for JSON Serializable arguments and returns.

        Args:
            ttl (Optional[int]): The number of seconds until the key expires.
                Defaults to None.
            **backend_kwargs: Additional keyword arguments to be passed to the cache
                backend's set method when storing values.

        Returns:
            Callable[..., SyncFunc[T]]: Decorated function.
        """
        cache_manager: CacheManager[T] = CacheManager.get_instance()

        def decorator(func: SyncFunc[T]) -> SyncFunc[T]:
            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                key = CacheManager.generate_callable_key(func, args, kwargs)

                with cache_manager._track_operation():
                    if result := cache_manager.backend.get(key=key):
                        logger.success(f"✅ Cache hit for key: {key}")
                        return cast(T, result)

                    logger.warning(f"❔ Cache miss for key: {key}")
                    result = func(*args, **kwargs)

                    if cache_manager.backend.set(
                        key=key, value=result, ttl=ttl, **backend_kwargs
                    ):
                        logger.info(
                            f"✅ Stored result in cache with key: {key} with TTL of "
                            + TimeCalculator.calculate_time_taken(0, float(ttl or 0))
                        )
                    else:
                        logger.error(
                            f"❌ Failed to store result in cache with key: {key}"
                        )
                    return cast(T, result)

            return sync_wrapper

        return decorator

    @staticmethod
    def async_cache_result(
        ttl: Optional[int] = config.get("CACHE.TTL"), **backend_kwargs: Any
    ) -> Callable[[AsyncFunc[T]], AsyncFunc[T]]:
        """
        📦 Decorator for caching function results. Works with both asynchronous functions.

        NOTE: This only works for JSON Serializable arguments and returns.

        Args:
            ttl (Optional[int]): The number of seconds until the key expires.
                Defaults to None.
            **backend_kwargs: Additional keyword arguments to be passed to the cache
                backend's set method when storing values.

        Returns:
            Callable[..., AsyncFunc[T]]: Decorated function.
        """
        cache_manager: CacheManager[T] = CacheManager.get_instance()

        def decorator(func: AsyncFunc[T]) -> AsyncFunc[T]:
            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                key = CacheManager.generate_callable_key(func, args, kwargs)

                with cache_manager._track_operation():
                    if result := cache_manager.backend.get(key=key):
                        logger.success(f"✅ Cache hit for key: {key}")
                        return cast(T, result)

                    logger.warning(f"❔ Cache miss for key: {key}")
                    result = await func(*args, **kwargs)

                    if cache_manager.backend.set(
                        key=key, value=result, ttl=ttl, **backend_kwargs
                    ):
                        logger.info(
                            f"✅ Stored result in cache with key: {key} with TTL of "
                            + TimeCalculator.calculate_time_taken(0, float(ttl or 0))
                        )
                    else:
                        logger.error(
                            f"❌ Failed to store result in cache with key: {key}"
                        )
                    return cast(T, result)

            return async_wrapper

        return decorator

    @staticmethod
    def generate_callable_key(
        func: Callable[..., T], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> str:
        """
        🔑 Generate a unique cache key for a callable function.

        This method creates a unique key based on the function name,
        sorted keyword arguments, and filtered positional arguments.
        The key is used to store and retrieve cached results, ensuring
        that different inputs produce different keys.

        Args:
            func (Callable[..., T]): The function for which the key is generated.
            args (tuple[Any, ...]): The positional arguments passed to the function.
            kwargs (dict[str, Any]): The keyword arguments passed to the function.

        Returns:
            str: A unique MD5 hash string representing the cache key.
        """
        key_properties = {
            "func": CacheManager.get_function_name(func),
            "kwargs": CacheManager.sort_dictionary(kwargs),
            "args": CacheManager.remove_self_or_cls_from_args(func, args),
        }
        return md5(dumps(key_properties)).hexdigest()

    @staticmethod
    def sort_dictionary(
        dictionary: dict[Any, Any],
        key: Optional[Callable[..., Any]] = None,
        reverse: bool = False,
    ) -> dict[Any, Any]:
        """
        📦 Sort a dictionary by its keys.

        Args:
            dictionary (dict[Any, Any]): The dictionary to be sorted.
            key (Optional[Callable[..., Any]]): A function to be called on each
                key prior to making comparisons. Defaults to None.
            reverse (bool): If set to True, then the list elements are sorted as
                if each comparison were reversed. Defaults to False.

        Returns:
            dict[Any, Any]: A sorted dictionary with the same keys and values as the
                input dictionary.
        """
        return dict(sorted(dictionary.items(), key=key, reverse=reverse))

    @staticmethod
    def remove_self_or_cls_from_args(
        func: Callable[..., Any], args: tuple[Any, ...]
    ) -> tuple[Any, ...]:
        """
        🗑️ Remove the `self` or `cls` argument from the argument tuple if present.

        This method checks if the first argument in the tuple is an instance or class
        method reference, and removes it accordingly.

        Args:
            func (Callable[..., Any]): The function whose arguments are being processed.
            args (tuple[Any, ...]): The tuple of arguments passed to the function.

        Returns:
            tuple[Any, ...]: A tuple of arguments with `self` or `cls` removed if
                applicable.
        """

        return (
            args[1:] if args and ismethod(getattr(args[0], func.__name__, None)) else args
        )

    @staticmethod
    def get_function_name(func: Callable[..., Any]) -> str:
        """
        🔎 Retrieve the fully qualified name of a function, including its module name.

        Args:
            func (Callable[..., Any]): The function whose name is to be retrieved.

        Returns:
            str: A string representing the full name of the function, in the format
            "module_name.function_name". If the module or function name cannot be
            determined, it defaults to "<unknown_module>" or the function's type
            name, respectively.
        """
        module = getattr(func, "__module__", "<unknown_module>")
        name = getattr(func, "__qualname__", type(func).__name__)
        return f"{module}.{name}"


class OperationTracker(Generic[T]):
    def __init__(self, cache_manager: CacheManager[T]) -> None:
        """
        🚀 Initialize the operation tracker with the cache manager.

        Args:
            cache_manager (CacheManager): The cache manager to track operations for.
        """
        self.cache_manager: CacheManager[T] = cache_manager

    def __enter__(self) -> None:
        """
        🚪 Enter the runtime context related to this object. The active operation counter
        of the cache manager is incremented by 1.
        """
        with self.cache_manager._active_operations_lock:
            self.cache_manager._active_operations += 1

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        """
        🚪 Exit the runtime context related to this object. The active operation counter
        of the cache manager is decremented by 1.
        """
        with self.cache_manager._active_operations_lock:
            self.cache_manager._active_operations -= 1
