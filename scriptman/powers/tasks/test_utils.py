"""
Test utilities and fixtures for the tasks module tests.
"""
import asyncio
import pickle
import time
from concurrent.futures import Future
from typing import Any, Callable
from unittest.mock import Mock, patch

import pytest

from scriptman.powers.tasks import Task, TaskManager, Tasks
from scriptman.powers.tasks._models import TaskException
from scriptman.powers.tasks._thread_executor import ThreadExecutor


# Test helper functions
def fast_function(value: int = 42) -> int:
    """Fast function for testing."""
    return value


def slow_function(delay: float = 0.1) -> str:
    """Slow function for testing."""
    time.sleep(delay)
    return f"completed after {delay}s"


def failing_function(error_msg: str = "Test error") -> None:
    """Function that always fails."""
    raise ValueError(error_msg)


async def async_function(value: str = "async_result") -> str:
    """Async function for testing."""
    await asyncio.sleep(0.01)
    return value


async def async_failing_function(error_msg: str = "Async error") -> None:
    """Async function that always fails."""
    await asyncio.sleep(0.01)
    raise RuntimeError(error_msg)


def memory_intensive_function(size_mb: int = 10) -> list[bytes]:
    """Function that allocates memory for testing."""
    return [b"x" * (1024 * 1024)] * size_mb


# Fixtures
@pytest.fixture
def task_manager():
    """Create a fresh TaskManager instance."""
    # Reset singleton state
    TaskManager._TaskManager__instance = None
    TaskManager._TaskManager__is_shutdown = False
    TaskManager._TaskManager__initialized = False
    return TaskManager()


@pytest.fixture
def thread_executor():
    """Create a ThreadExecutor instance."""
    return ThreadExecutor(max_workers=2, idle_timeout=1)


@pytest.fixture
def completed_future():
    """Create a completed Future."""
    future = Future()
    future.set_result("test_result")
    return future


@pytest.fixture
def failed_future():
    """Create a failed Future."""
    future = Future()
    future.set_exception(ValueError("test_error"))
    return future


@pytest.fixture
def pending_future():
    """Create a pending Future."""
    return Future()


# Test utilities
def assert_timing(actual_time: float, expected_time: float, tolerance: float = 0.1) -> None:
    """Assert that timing is within tolerance."""
    assert abs(actual_time - expected_time) <= tolerance


def assert_memory_usage(func: Callable, max_mb: int = 100) -> None:
    """Assert that function doesn't use excessive memory."""
    import psutil
    import os

    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / (1024 * 1024)

    result = func()

    final_memory = process.memory_info().rss / (1024 * 1024)
    memory_used = final_memory - initial_memory

    assert memory_used <= max_mb, f"Memory usage {memory_used:.2f}MB exceeds limit {max_mb}MB"
    return result


def create_mock_future(result: Any = None, exception: Exception | None = None) -> Mock:
    """Create a mock Future for testing."""
    future = Mock(spec=Future)
    future.done.return_value = exception is not None or result is not None
    future.cancelled.return_value = False
    future.exception.return_value = exception
    future.result.return_value = result
    return future
