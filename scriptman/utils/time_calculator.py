from contextlib import contextmanager
from time import perf_counter
from typing import Callable

from loguru import logger


class TimeCalculator:
    """
    🕰️ A utility class for calculating time taken in different contexts.
    """

    @staticmethod
    def calculate_time_taken(start_time: float, end_time: float) -> str:
        """
        🕰️ Calculate the time taken between two timestamps and return a human-readable
        string.

        Args:
            start_time (float): The start time as a Unix timestamp.
            end_time (float): The end time as a Unix timestamp.

        Returns:
            str: A human-readable string like "1 day 2 hours 3 minutes 4 seconds".
        """
        time_taken = end_time - start_time

        # Calculate days, hours, minutes, and seconds
        days = time_taken // 86400
        hours = (time_taken % 86400) // 3600
        minutes = (time_taken % 3600) // 60
        seconds = time_taken % 60

        # Build the human-readable format
        time_parts = []

        if days > 0:
            time_parts.append(f"{int(days)} day{'s' if days > 1 else ''}")
        if hours > 0:
            time_parts.append(f"{int(hours)} hour{'s' if hours > 1 else ''}")
        if minutes > 0:
            time_parts.append(f"{int(minutes)} minute{'s' if minutes > 1 else ''}")
        if seconds > 0 or time_taken < 60:  # Show seconds if less than a minute
            time_parts.append(f"{int(seconds)} second{'s' if seconds > 1 else ''}")

        # Join all the parts with commas and 'and' before the last item if necessary
        if len(time_parts) > 1:
            return ", ".join(time_parts[:-1]) + " and " + time_parts[-1]
        else:
            return time_parts[0]

    @staticmethod
    def calculate_time_for_function(func: Callable) -> Callable:
        """
        🕰️ A decorator to calculate and display the time taken by a function.

        Args:
            func (Callable): The function to be wrapped.

        Returns:
            Callable: The wrapped function.
        """

        def wrapper(*args, **kwargs):
            start_time = perf_counter()
            result = func(*args, **kwargs)
            time_taken = TimeCalculator.calculate_time_taken(start_time, perf_counter())
            logger.info(f"{func.__name__} took {time_taken} to execute.")
            return result

        return wrapper

    @contextmanager
    @staticmethod
    def time_context_manager(context_name: str = "Code Block"):
        """
        A context manager to calculate and display the time taken within a context.
        """
        start_time = perf_counter()
        try:
            yield
        finally:
            time_taken = TimeCalculator.calculate_time_taken(start_time, perf_counter())
            logger.info(f"{context_name} took {time_taken} to execute.")
