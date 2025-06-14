from functools import wraps
from inspect import iscoroutinefunction
from time import sleep
from typing import Callable, Optional, Type, Union, cast

from loguru import logger

from scriptman.core.config import config
from scriptman.powers.generics import P, R
from scriptman.powers.tasks import TaskExecutor


def retry(
    max_retries: int = config.settings.get("retry.max_retries", 1),
    base_delay: float = config.settings.get("retry.base_delay", 1),
    min_delay: float = config.settings.get("retry.min_delay", 1),
    max_delay: float = config.settings.get("retry.max_delay", 10),
    retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    🔁 A unified retry decorator for both synchronous and asynchronous functions.

    Args:
        max_retries (int): Maximum number of retry attempts.
        base_delay (float): Base delay for exponential backoff.
        min_delay (float): Minimum delay between retries.
        max_delay (float): Maximum delay between retries.
        retry_on (Optional[Union[Type[Exception], tuple[Type[Exception], ...]]]):
            Exception or tuple of exceptions to retry on.

    Returns:
        A decorator that applies retry logic to either a sync or async function.
    """
    __validate_retry_params(base_delay, min_delay, max_delay, max_retries)

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(max_retries + 1):
                try:
                    if iscoroutinefunction(func):
                        logger.debug("🔄 Trying async function")
                        result = TaskExecutor.await_async(func(*args, **kwargs))
                    else:
                        logger.debug("🔄 Trying sync function")
                        result = func(*args, **kwargs)

                    return cast(R, result)
                except Exception as e:
                    delay = __get_delay(
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

        return wrapper

    return decorator


def __validate_retry_params(
    base_delay: float, min_delay: float, max_delay: float, max_retries: int
) -> None:
    """Validate the retry parameters."""
    max_ge_min: bool = max_delay >= min_delay
    assert base_delay > 0, f"base_delay must be positive, got {base_delay}"
    assert min_delay >= 0, f"min_delay must be non-negative, got {min_delay}"
    assert max_retries >= 0, f"max_retries must be non-negative, got {max_retries}"
    assert max_ge_min, f"max_delay ({max_delay}) must be >= min_delay ({min_delay})"


def __get_delay(
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
