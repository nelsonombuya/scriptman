from asyncio import iscoroutinefunction, run, sleep
from functools import wraps
from typing import Any, Callable, Optional, Type, Union, cast

from loguru import logger

from scriptman.core.config import config
from scriptman.powers.generics import AsyncFunc, Func, SyncFunc, T


def retry(
    max_retries: int = config.get("RETRY.MAX_RETRIES", 1),
    base_delay: float = config.get("RETRY.BASE_DELAY", 1),
    min_delay: float = config.get("RETRY.MIN_DELAY", 1),
    max_delay: float = config.get("RETRY.MAX_DELAY", 10),
    retry_on: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
) -> Callable[..., Func[T]]:
    """
    🔁 A simple retry decorator supporting both sync and async functions

    Args:
        max_retries (int): Maximum number of retry attempts. Defaults to 1.
        base_delay (float): Base delay for exponential backoff. Defaults to 1.
        min_delay (float): Minimum delay between retry attempts. Defaults to 1.
        max_delay (float): Maximum delay between retry attempts. Defaults to 10.
        retry_on (Optional[Union[Type[Exception], tuple[Type[Exception], ...]]]):
            Exception or tuple of exceptions to retry on. Defaults to None.

    Returns:
        Decorated function with retry capabilities
    """
    # Validate input parameters
    max_ge_min: bool = max_delay >= min_delay
    assert base_delay > 0, f"base_delay must be positive, got {base_delay}"
    assert min_delay >= 0, f"min_delay must be non-negative, got {min_delay}"
    assert max_retries >= 0, f"max_retries must be non-negative, got {max_retries}"
    assert max_ge_min, f"max_delay ({max_delay}) must be >= min_delay ({min_delay})"

    def decorator(func: Func[T]) -> Func[T]:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> T:
            for attempt in range(max_retries + 1):
                try:
                    if iscoroutinefunction(func):
                        return await cast(AsyncFunc[T], func)(*args, **kwargs)
                    else:
                        return cast(SyncFunc[T], func)(*args, **kwargs)
                except Exception as e:
                    # Check if we should retry
                    if retry_on is not None and not isinstance(e, retry_on):
                        logger.warning(f"⚠ Skipping retry for exception: {e}")
                        raise e

                    # Don't retry on the last attempt
                    if attempt >= max_retries:
                        logger.error(f"❌ Failed after {attempt} attempts: {e}")
                        raise e

                    # Apply delay
                    delay = min(max(base_delay * (2**attempt), min_delay), max_delay)
                    logger.warning(f"⏳ Retrying in {delay} seconds... ({e})")
                    await sleep(delay)

            raise RuntimeError(f"❌ Failed after {max_retries} attempts")

        # If the original function is synchronous, return a synchronous wrapper
        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> T:
            return run(async_wrapper(*args, **kwargs))

        return async_wrapper if iscoroutinefunction(func) else sync_wrapper

    return decorator
