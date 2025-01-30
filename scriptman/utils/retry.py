from asyncio import iscoroutinefunction, run, sleep
from contextlib import asynccontextmanager, contextmanager
from functools import wraps
from typing import Any, Callable, Optional, Type, Union, cast
from loguru import logger

from scriptman.core.config import config
from scriptman.utils.generics import AsyncFunc, SyncFunc, T


class Retry:
    """
    🔁 A simple retry class supporting both sync and async functions with exponential
    backoff.
    """

    def __init__(
        self,
        max_retries: int = config.env.get("RETRY.MAX_RETRIES", 1),
        base_delay: float = config.env.get("RETRY.BASE_DELAY", 1),
        min_delay: float = config.env.get("RETRY.MIN_DELAY", 1),
        max_delay: float = config.env.get("RETRY.MAX_DELAY", 10),
        retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
    ):
        """
        🚀 Initialize retry configuration.

        Args:
            max_retries (int): Maximum number of retry attempts. Defaults to 1.
            base_delay (float): Base delay for exponential backoff. Defaults to 1.
            min_delay (float): Minimum delay between retry attempts. Defaults to 1.
            max_delay (float): Maximum delay between retry attempts. Defaults to 10.
            retry_on (Optional[Union[Type[Exception], tuple[Type[Exception], ...]]]):
                Exception or tuple of exceptions to retry on. Defaults to None.
        """
        # Validate the parameters
        max_ge_min = max_delay >= min_delay
        assert base_delay > 0, f"base_delay must be positive, got {base_delay}"
        assert min_delay >= 0, f"min_delay must be non-negative, got {min_delay}"
        assert max_retries >= 0, f"max_retries must be non-negative, got {max_retries}"
        assert max_ge_min, f"max_delay ({max_delay}) must be >= min_delay ({min_delay})"

        self.max_retries = max_retries
        self.base_delay = base_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.retry_on = retry_on

    async def _retry(
        self,
        func: SyncFunc[T] | AsyncFunc[T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """🔁 Execute function with retry logic."""
        for attempt in range(self.max_retries + 1):
            try:
                if iscoroutinefunction(func):
                    return await cast(AsyncFunc[T], func)(*args, **kwargs)
                return cast(SyncFunc[T], func)(*args, **kwargs)
            except Exception as e:
                logger.warning(f"⚠️ Retrying function {func.__qualname__}... ({e})")
                await self._handle_exception(e, attempt)
        raise RuntimeError(f"❌ Failed after {self.max_retries} attempts")

    async def _handle_exception(self, e: Exception, attempt: int) -> None:
        """❌ Handle exceptions during retry attempts."""
        if self.retry_on is not None and not isinstance(e, self.retry_on):
            logger.warning(f"⚠️ Skipping retry for exception: {e}")
            logger.error(f"❌ Failed after {attempt} attempts: {e}")
            raise e

        if attempt >= self.max_retries:
            logger.error(f"❌ Failed after {attempt} attempts: {e}")
            raise e

        delay = min(max(self.base_delay * (2**attempt), self.min_delay), self.max_delay)
        logger.warning(f"⏳ Retrying in {delay} seconds... ({e})")
        await sleep(delay)

    @staticmethod
    def decorator(
        max_retries: int = config.env.get("RETRY.MAX_RETRIES", 1),
        base_delay: float = config.env.get("RETRY.BASE_DELAY", 1),
        min_delay: float = config.env.get("RETRY.MIN_DELAY", 1),
        max_delay: float = config.env.get("RETRY.MAX_DELAY", 10),
        retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
    ) -> Callable:
        """
        🎯 Decorator for adding retry capability to functions.

        Args:
            max_retries (int): Maximum number of retry attempts. Defaults to 1.
            base_delay (float): Base delay for exponential backoff. Defaults to 1.
            min_delay (float): Minimum delay between retry attempts. Defaults to 1.
            max_delay (float): Maximum delay between retry attempts. Defaults to 10.
            retry_on (Optional[Union[Type[Exception], tuple[Type[Exception], ...]]]):
                Exception or tuple of exceptions to retry on. Defaults to None.

        Usage:
            @Retry.decorator(max_retries=3)
            async def my_func():
                ...
        """

        def wrapper(func: SyncFunc[T] | AsyncFunc[T]) -> SyncFunc[T] | AsyncFunc[T]:
            instance = Retry(
                max_retries=max_retries,
                base_delay=base_delay,
                min_delay=min_delay,
                max_delay=max_delay,
                retry_on=retry_on,
            )

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                return await instance._retry(func, *args, **kwargs)

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                return run(async_wrapper(*args, **kwargs))

            return async_wrapper if iscoroutinefunction(func) else sync_wrapper

        return wrapper

    @contextmanager
    @staticmethod
    def context(
        context: str = "Code Block",
        max_retries: int = config.env.get("RETRY.MAX_RETRIES", 1),
        base_delay: float = config.env.get("RETRY.BASE_DELAY", 1),
        min_delay: float = config.env.get("RETRY.MIN_DELAY", 1),
        max_delay: float = config.env.get("RETRY.MAX_DELAY", 10),
        retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
    ):
        instance = Retry(max_retries, base_delay, min_delay, max_delay, retry_on)
        for attempt in range(instance.max_retries + 1):
            try:
                yield
                return
            except Exception as e:
                logger.warning(f"⚠️ Retrying context {context}... ({e})")
                run(instance._handle_exception(e, attempt))
        raise RuntimeError(f"❌ Failed after {instance.max_retries} attempts")

    @asynccontextmanager
    @staticmethod
    async def async_context(
        context: str = "Code Block",
        max_retries: int = config.env.get("RETRY.MAX_RETRIES", 1),
        base_delay: float = config.env.get("RETRY.BASE_DELAY", 1),
        min_delay: float = config.env.get("RETRY.MIN_DELAY", 1),
        max_delay: float = config.env.get("RETRY.MAX_DELAY", 10),
        retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
    ):
        instance = Retry(max_retries, base_delay, min_delay, max_delay, retry_on)
        for attempt in range(instance.max_retries + 1):
            try:
                yield
                return
            except Exception as e:
                logger.warning(f"⚠️ Retrying context {context}... ({e})")
                await instance._handle_exception(e, attempt)
        raise RuntimeError(f"❌ Failed after {instance.max_retries} attempts")
