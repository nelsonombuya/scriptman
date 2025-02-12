from asyncio import sleep as async_sleep
from functools import wraps
from time import sleep as sync_sleep
from typing import Any, Callable, Optional, Type, Union

from loguru import logger

from scriptman.core.config import config
from scriptman.powers.generics import AsyncFunc, SyncFunc, T


def retry(
    max_retries: int = config.get("RETRY.MAX_RETRIES", 1),
    base_delay: float = config.get("RETRY.BASE_DELAY", 1),
    min_delay: float = config.get("RETRY.MIN_DELAY", 1),
    max_delay: float = config.get("RETRY.MAX_DELAY", 10),
    retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
) -> Callable[[SyncFunc[T]], SyncFunc[T]]:
    """
    🔁 A retry decorator for synchronous functions.

    Args:
        max_retries (int): Maximum number of retry attempts.
        base_delay (float): Base delay for exponential backoff.
        min_delay (float): Minimum delay between retries.
        max_delay (float): Maximum delay between retries.
        retry_on (Optional[Union[Type[Exception], tuple[Type[Exception], ...]]]):
            Exception or tuple of exceptions to retry on.

    Returns:
        A decorator that applies retry logic to a sync function.
    """

    def decorator(func: SyncFunc[T]) -> SyncFunc[T]:
        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> T:
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # If a specific exception type is set and e is not it, do not retry
                    if retry_on is not None and not isinstance(e, retry_on):
                        logger.warning(f"⚠ Skipping retry for exception: {e}")
                        raise e

                    # On the last attempt, log and re-raise
                    if attempt >= max_retries:
                        logger.error(f"❌ Failed after {attempt} attempts: {e}")
                        raise e

                    # Calculate delay using exponential backoff
                    delay = min(max(base_delay * (2**attempt), min_delay), max_delay)
                    logger.warning(
                        f"⏳ Retrying in {delay} seconds... (attempt {attempt + 1}) - {e}"
                    )
                    sync_sleep(delay)
            raise RuntimeError(f"❌ Failed after {max_retries} attempts")

        return sync_wrapper

    return decorator


def async_retry(
    max_retries: int = config.get("RETRY.MAX_RETRIES", 1),
    base_delay: float = config.get("RETRY.BASE_DELAY", 1),
    min_delay: float = config.get("RETRY.MIN_DELAY", 1),
    max_delay: float = config.get("RETRY.MAX_DELAY", 10),
    retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
) -> Callable[[AsyncFunc[T]], AsyncFunc[T]]:
    """
    🔁 A retry decorator for asynchronous functions.

    Args:
        max_retries (int): Maximum number of retry attempts.
        base_delay (float): Base delay for exponential backoff.
        min_delay (float): Minimum delay between retries.
        max_delay (float): Maximum delay between retries.
        retry_on (Optional[Union[Type[Exception], tuple[Type[Exception], ...]]]):
            Exception or tuple of exceptions to retry on.

    Returns:
        A decorator that applies retry logic to an async function.
    """
    # Validate input parameters
    max_ge_min: bool = max_delay >= min_delay
    assert base_delay > 0, f"base_delay must be positive, got {base_delay}"
    assert min_delay >= 0, f"min_delay must be non-negative, got {min_delay}"
    assert max_retries >= 0, f"max_retries must be non-negative, got {max_retries}"
    assert max_ge_min, f"max_delay ({max_delay}) must be >= min_delay ({min_delay})"

    def decorator(func: AsyncFunc[T]) -> AsyncFunc[T]:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> T:
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    # If a specific exception type is set and e is not it, do not retry
                    if retry_on is not None and not isinstance(e, retry_on):
                        logger.warning(f"⚠ Skipping retry for exception: {e}")
                        raise e

                    # On the last attempt, log and re-raise
                    if attempt >= max_retries:
                        logger.error(f"❌ Failed after {attempt} attempts: {e}")
                        raise e

                    # Calculate delay using exponential backoff
                    delay = min(max(base_delay * (2**attempt), min_delay), max_delay)
                    logger.warning(
                        f"⏳ Retrying in {delay} seconds... (attempt {attempt + 1}) - {e}"
                    )
                    await async_sleep(delay)
            raise RuntimeError(f"❌ Failed after {max_retries} attempts")

        return async_wrapper

    return decorator
