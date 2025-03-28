from functools import wraps
from typing import Callable, Optional, Type, Union

from loguru import logger

from scriptman.core.config import config
from scriptman.powers.generics import AsyncFunc, P, R, SyncFunc


def retry(
    max_retries: int = config.settings.get("retry.max_retries", 1),
    base_delay: float = config.settings.get("retry.base_delay", 1),
    min_delay: float = config.settings.get("retry.min_delay", 1),
    max_delay: float = config.settings.get("retry.max_delay", 10),
    retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
) -> Callable[[SyncFunc[P, R]], SyncFunc[P, R]]:
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
    from time import sleep

    _validate_retry_params(base_delay, min_delay, max_delay, max_retries)

    def decorator(func: SyncFunc[P, R]) -> SyncFunc[P, R]:
        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    delay = _get_delay(
                        e,
                        retry_on,
                        attempt,
                        max_retries,
                        base_delay,
                        min_delay,
                        max_delay,
                    )
                    sleep(delay)
            raise RuntimeError(f"❌ Failed after {max_retries} attempts")

        return sync_wrapper

    return decorator


def async_retry(
    max_retries: int = config.settings.get("retry.max_retries", 1),
    base_delay: float = config.settings.get("retry.base_delay", 1),
    min_delay: float = config.settings.get("retry.min_delay", 1),
    max_delay: float = config.settings.get("retry.max_delay", 10),
    retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
) -> Callable[[AsyncFunc[P, R]], AsyncFunc[P, R]]:
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
    from asyncio import sleep

    _validate_retry_params(base_delay, min_delay, max_delay, max_retries)

    def decorator(func: AsyncFunc[P, R]) -> AsyncFunc[P, R]:
        @wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    delay = _get_delay(
                        e,
                        retry_on,
                        attempt,
                        max_retries,
                        base_delay,
                        min_delay,
                        max_delay,
                    )
                    await sleep(delay)
            raise RuntimeError(f"❌ Failed after {max_retries} attempts")

        return async_wrapper

    return decorator


def _validate_retry_params(
    base_delay: float, min_delay: float, max_delay: float, max_retries: int
) -> None:
    """Validate the retry parameters."""
    max_ge_min: bool = max_delay >= min_delay
    assert base_delay > 0, f"base_delay must be positive, got {base_delay}"
    assert min_delay >= 0, f"min_delay must be non-negative, got {min_delay}"
    assert max_retries >= 0, f"max_retries must be non-negative, got {max_retries}"
    assert max_ge_min, f"max_delay ({max_delay}) must be >= min_delay ({min_delay})"


def _get_delay(
    e: Exception,
    retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]],
    attempt: int,
    max_retries: int,
    base_delay: float,
    min_delay: float,
    max_delay: float,
) -> float:
    # If a specific exception type is set and e is not it, do not retry
    if retry_on is not None and not isinstance(e, retry_on):
        logger.debug(f"⚠ Skipping retry for exception: {e}")
        raise e

    # On the last attempt, log and re-raise
    if attempt >= max_retries:
        logger.error(f"❌ Failed after {attempt} attempts: {e}")
        raise e

    # Calculate delay using exponential backoff
    delay = float(min(max(base_delay * (2**attempt), min_delay), max_delay))
    logger.info(f"⏳ Retrying in {delay} seconds... (attempt {attempt + 1}) - {e}")
    return delay
