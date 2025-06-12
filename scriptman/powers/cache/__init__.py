try:
    from functools import wraps
    from hashlib import md5
    from inspect import iscoroutinefunction, ismethod
    from threading import Lock
    from typing import Any, Callable, Optional, cast

    from dill import dumps
    from loguru import logger

    from scriptman.core.config import config
    from scriptman.powers.cache._backend import CacheBackend
    from scriptman.powers.cache._diskcache import DiskCacheBackend, FanoutCacheBackend
    from scriptman.powers.generics import P, R
    from scriptman.powers.serializer import SERIALIZE_FOR_CACHE, serialize
    from scriptman.powers.time_calculator import TimeCalculator
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[cache]"
    )


class CacheManager:
    """Thread-safe singleton cache manager with safe backend switching capabilities"""

    _active_operations: int = 0
    _active_operations_lock: Lock = Lock()

    __lock: Lock = Lock()
    __backend: CacheBackend
    __initialized: bool = False
    __backend_switch_lock: Lock = Lock()
    __instance: Optional["CacheManager"] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "CacheManager":
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
    def get_instance(cls, *args: Any, **kwargs: Any) -> "CacheManager":
        """🚀 Get the singleton instance of CacheManager"""
        if cls.__instance is None:
            with cls.__lock:
                if cls.__instance is None:
                    cls.__instance = CacheManager(*args, **kwargs)
        return cls.__instance

    def _track_operation(self) -> "OperationTracker":
        """🔎 Context manager to track active cache operations"""
        return OperationTracker(self)

    def get(self, key: str, **backend_kwargs: Any) -> Any:
        """🔍 Get a value from the cache."""
        if key in backend_kwargs:
            backend_kwargs.pop(key)  # Remove the key from the backend kwargs
        return self.backend.get(key, **backend_kwargs)

    def set(
        self, key: str, value: Any, ttl: Optional[int] = None, **backend_kwargs: Any
    ) -> bool:
        """🔍 Set a value in the cache."""
        if "ttl" in backend_kwargs:
            backend_kwargs.pop("ttl")  # Remove the ttl from the backend kwargs
        return self.backend.set(key, value, ttl, **backend_kwargs)

    def delete(self, key: str, **backend_kwargs: Any) -> bool:
        """🔍 Delete a value from the cache."""
        if key in backend_kwargs:
            backend_kwargs.pop(key)  # Remove the key from the backend kwargs
        return self.backend.delete(key, **backend_kwargs)

    @staticmethod
    def cache_result(
        ttl: Optional[int] = config.settings.get("cache.ttl"),
        **backend_kwargs: Any,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """
        📦 Decorator for caching function results. Works with both sync and async
        functions.

        NOTE: This only works for JSON Serializable arguments and returns.

        Args:
            ttl (Optional[int]): The number of seconds until the key expires.
                Defaults to None.
            **backend_kwargs: Additional keyword arguments to be passed to the cache
                backend's set method when storing values.

        Returns:
            A decorator that caches the function's results.
        """
        cache = CacheManager.get_instance()

        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            @wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                key = cache.generate_callable_key(func, args, kwargs)

                with cache._track_operation():
                    if result := cache.get(key=key):
                        logger.debug(f"✅ Cache hit for key: {key}")
                        return cast(R, result)

                    logger.info(f"❔ Cache miss for key: {key}")
                    if iscoroutinefunction(func):
                        logger.debug("🔄 Executing async cached function")
                        from scriptman.powers.concurrency import TaskExecutor

                        result = TaskExecutor.await_async(func(*args, **kwargs))
                    else:
                        logger.debug("🔄 Executing sync cached function")
                        result = func(*args, **kwargs)

                    if not result:
                        logger.debug(f"❔ Skipping cache for key {key}: result is empty")
                        return cast(R, result)

                    if cache.set(key=key, value=result, ttl=ttl, **backend_kwargs):
                        ttl_str = TimeCalculator.calculate_time_taken(0, float(ttl or 0))
                        logger.debug(f"✅ Stored {key} in cache for {ttl_str}")
                    else:
                        logger.error(f"❌ Failed to store {key} in cache")

                    return cast(R, result)

            return wrapper

        return decorator

    def generate_callable_key(
        self, func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
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
            "func": self.get_function_name(func),
            "kwargs": self.sort_dictionary(kwargs),
            "args": self.remove_self_or_cls_from_args(func, args),
        }
        return md5(dumps(key_properties)).hexdigest()

    def sort_dictionary(
        self,
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
        return dict(
            sorted(
                dict(serialize(dictionary, **SERIALIZE_FOR_CACHE)).items(),
                reverse=reverse,
                key=key,
            )
        )

    def remove_self_or_cls_from_args(
        self, func: Callable[..., Any], args: tuple[Any, ...]
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
        return tuple(
            serialize(
                args[1:]
                if args and ismethod(getattr(args[0], func.__name__, None))
                else args
            )
        )

    def get_function_name(self, func: Callable[..., Any]) -> str:
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


class OperationTracker:
    def __init__(self, cache_manager: CacheManager) -> None:
        """
        🚀 Initialize the operation tracker with the cache manager.

        Args:
            cache_manager (CacheManager): The cache manager to track operations for.
        """
        self.cache_manager: CacheManager = cache_manager

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


cache: CacheManager = CacheManager.get_instance()

__all__: list[str] = [
    "CacheManager",
    "OperationTracker",
    "CacheBackend",
    "DiskCacheBackend",
    "FanoutCacheBackend",
]
