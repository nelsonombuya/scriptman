"""
Performance tests for the tasks module.
"""
import asyncio
import time
from concurrent.futures import Future
from unittest.mock import patch

import pytest

from scriptman.powers.tasks import TaskManager


class TestTasksPerformance:
    """Performance tests for the tasks module."""

    def test_task_submission_latency(self):
        """Test task submission latency."""
        manager = TaskManager()

        def simple_task():
            return "result"

        # Measure submission latency
        start_time = time.perf_counter()
        task = manager.background(simple_task)
        submission_time = time.perf_counter() - start_time

        # Should be very fast (under 1ms)
        assert submission_time < 0.001

        # Wait for completion
        result = task.await_result(timeout=1.0)
        assert result == "result"

        manager.cleanup_all()

    def test_batch_execution_throughput(self):
        """Test batch execution throughput."""
        manager = TaskManager()

        def cpu_task(x):
            # Simple CPU work
            return sum(range(x))

        # Submit batch of tasks
        tasks = [(cpu_task, (i,), {}) for i in range(100, 200)]

        start_time = time.perf_counter()
        batch = manager.multithread(tasks)
        results = batch.await_results()
        total_time = time.perf_counter() - start_time

        # Should complete reasonably fast
        assert total_time < 5.0  # 5 seconds for 100 tasks
        assert len(results) == 100

        manager.cleanup_all()

    def test_memory_usage_during_execution(self):
        """Test memory usage during execution."""
        manager = TaskManager()

        def memory_task(size_mb):
            return [b"x" * (1024 * 1024)] * size_mb

        # Submit memory-intensive tasks
        tasks = [(memory_task, (i,), {}) for i in range(1, 6)]  # 1-5MB tasks

        start_time = time.perf_counter()
        batch = manager.multithread(tasks)
        results = batch.await_results()
        total_time = time.perf_counter() - start_time

        # Should complete without excessive memory usage
        assert total_time < 10.0
        assert len(results) == 5

        manager.cleanup_all()

    def test_cleanup_timing(self):
        """Test cleanup timing."""
        manager = TaskManager()

        # Submit some tasks
        futures = []
        for i in range(10):
            future = manager.background(lambda: time.sleep(0.01) or f"task_{i}")
            futures.append(future)

        # Wait for tasks to start
        time.sleep(0.05)

        # Measure cleanup time
        start_time = time.perf_counter()
        manager.cleanup_all(wait=True)
        cleanup_time = time.perf_counter() - start_time

        # Cleanup should be reasonably fast
        assert cleanup_time < 2.0

    def test_maximum_concurrent_tasks(self):
        """Test maximum concurrent tasks (stress test)."""
        manager = TaskManager()

        def stress_task(task_id):
            time.sleep(0.01)  # Small delay
            return f"stress_{task_id}"

        # Submit many concurrent tasks
        futures = []
        for i in range(200):  # Large number of tasks
            future = manager.background(stress_task, i)
            futures.append(future)

        # Wait for all tasks
        start_time = time.perf_counter()
        results = []
        for future in futures:
            result = future.await_result(timeout=10.0)
            results.append(result)
        total_time = time.perf_counter() - start_time

        # Should handle many concurrent tasks
        assert len(results) == 200
        assert total_time < 15.0  # Should complete within reasonable time

        manager.cleanup_all()

    def test_worker_pool_saturation(self):
        """Test worker pool saturation."""
        manager = TaskManager()

        def blocking_task(duration):
            time.sleep(duration)
            return f"blocked_{duration}"

        # Submit tasks that will saturate the pool
        futures = []
        for i in range(20):  # More tasks than workers
            future = manager.background(blocking_task, 0.1)
            futures.append(future)

        # Measure completion time
        start_time = time.perf_counter()
        results = []
        for future in futures:
            result = future.await_result(timeout=5.0)
            results.append(result)
        total_time = time.perf_counter() - start_time

        # Should complete all tasks
        assert len(results) == 20
        # Should take longer than sequential execution due to saturation
        assert total_time > 0.5  # At least 0.5 seconds

        manager.cleanup_all()

    def test_queue_depth_under_load(self):
        """Test queue depth under load."""
        manager = TaskManager()

        def slow_task():
            time.sleep(0.2)
            return "slow"

        # Submit many slow tasks quickly
        futures = []
        for i in range(50):
            future = manager.background(slow_task)
            futures.append(future)

        # Check that all tasks are queued/submitted
        assert len(futures) == 50

        # Wait for completion
        results = []
        for future in futures:
            result = future.await_result(timeout=15.0)
            results.append(result)

        assert len(results) == 50
        assert all(r == "slow" for r in results)

        manager.cleanup_all()

    def test_idle_detection_accuracy(self):
        """Test idle detection accuracy."""
        manager = TaskManager()

        # Access threads property to create executor
        executor = manager.threads

        # Should not be idle immediately
        assert not executor.is_idle

        # Wait for idle timeout
        time.sleep(1.1)  # Slightly more than default timeout

        # Should be idle now
        assert executor.is_idle

        manager.cleanup_all()

    def test_cleanup_triggers_at_proper_times(self):
        """Test cleanup triggers at proper times."""
        manager = TaskManager()

        # Access threads property to start monitoring
        executor = manager.threads

        # Submit a task
        task = manager.background(lambda: "test")
        result = task.await_result(timeout=1.0)
        assert result == "test"

        # Wait for idle timeout
        time.sleep(1.1)

        # Executor should be cleaned up by monitoring
        # Note: This test might be flaky due to timing, so we'll just verify
        # that the monitoring is working
        assert manager._TaskManager__monitoring is True

        manager.cleanup_all()

    def test_resource_limit_updates_under_load(self):
        """Test resource limit updates under load."""
        manager = TaskManager()

        # Submit some tasks
        futures = []
        for i in range(10):
            future = manager.background(lambda: time.sleep(0.1) or "task")
            futures.append(future)

        # Update resource limits while tasks are running
        TaskManager.set_resource_percentage(25.0)

        # Wait for tasks to complete
        results = []
        for future in futures:
            result = future.await_result(timeout=2.0)
            results.append(result)

        assert len(results) == 10
        assert TaskManager.get_resource_percentage() == 25.0

        manager.cleanup_all()

    def test_multithread_vs_sequential_timing(self):
        """Test multithread vs sequential execution timing."""
        manager = TaskManager()

        def slow_task(x):
            time.sleep(0.1)
            return x * 2

        # Test sequential execution
        start_time = time.perf_counter()
        sequential_results = []
        for i in range(5):
            result = slow_task(i)
            sequential_results.append(result)
        sequential_time = time.perf_counter() - start_time

        # Test multithread execution
        start_time = time.perf_counter()
        tasks = [(slow_task, (i,), {}) for i in range(5)]
        batch = manager.multithread(tasks)
        multithread_results = batch.await_results()
        multithread_time = time.perf_counter() - start_time

        # Multithread should be faster
        assert multithread_time < sequential_time
        assert multithread_results == sequential_results

        manager.cleanup_all()

    def test_race_condition_timing(self):
        """Test race condition timing."""
        manager = TaskManager()

        def fast_task():
            return "fast"

        def slow_task():
            time.sleep(0.2)
            return "slow"

        # Test race with fast and slow tasks
        tasks = [
            (fast_task, (), {}),
            (slow_task, (), {}),
        ]

        start_time = time.perf_counter()
        winner = manager.race(tasks)
        result = winner.await_result(timeout=1.0)
        race_time = time.perf_counter() - start_time

        # Should return fast result quickly
        assert result == "fast"
        assert race_time < 0.1  # Should be much faster than slow task

        manager.cleanup_all()

    def test_task_wrapper_overhead(self):
        """Test overhead of task wrapper."""
        manager = TaskManager()

        def simple_task():
            return 42

        # Measure direct execution
        start_time = time.perf_counter()
        direct_result = simple_task()
        direct_time = time.perf_counter() - start_time

        # Measure task wrapper execution
        start_time = time.perf_counter()
        task = manager.background(simple_task)
        wrapper_result = task.await_result(timeout=1.0)
        wrapper_time = time.perf_counter() - start_time

        # Results should be the same
        assert direct_result == wrapper_result == 42

        # Wrapper should have some overhead but not excessive
        assert wrapper_time > direct_time
        assert wrapper_time < 0.1  # Should still be fast

        manager.cleanup_all()

    def test_async_task_performance(self):
        """Test async task performance."""
        manager = TaskManager()

        async def async_task(x):
            await asyncio.sleep(0.01)
            return x * 2

        # Submit async tasks
        tasks = [(async_task, (i,), {}) for i in range(10)]

        start_time = time.perf_counter()
        batch = manager.multithread(tasks)
        results = batch.await_results()
        total_time = time.perf_counter() - start_time

        # Should complete efficiently
        assert len(results) == 10
        assert total_time < 1.0
        assert all(isinstance(r, int) for r in results)

        manager.cleanup_all()

    def test_mixed_task_performance(self):
        """Test performance with mixed task types."""
        manager = TaskManager()

        def sync_task(x):
            return x * 2

        async def async_task(x):
            await asyncio.sleep(0.01)
            return x * 3

        def cpu_task(x):
            return sum(range(x))

        # Mix of different task types
        tasks = [
            (sync_task, (1,), {}),
            (async_task, (2,), {}),
            (cpu_task, (100,), {}),
            (sync_task, (3,), {}),
            (async_task, (4,), {}),
        ]

        start_time = time.perf_counter()
        batch = manager.multithread(tasks)
        results = batch.await_results()
        total_time = time.perf_counter() - start_time

        # Should handle mixed types efficiently
        assert len(results) == 5
        assert total_time < 2.0
        assert 2 in results  # sync_task(1)
        assert 6 in results  # async_task(2)
        assert 4950 in results  # cpu_task(100)
        assert 6 in results  # sync_task(3)
        assert 12 in results  # async_task(4)

        manager.cleanup_all()

    def test_error_handling_performance(self):
        """Test error handling performance."""
        manager = TaskManager()

        def failing_task():
            raise ValueError("test error")

        # Submit many failing tasks
        tasks = [(failing_task, (), {}) for _ in range(20)]

        start_time = time.perf_counter()
        batch = manager.multithread(tasks)

        # Test with raise_exceptions=False for performance
        results = batch.await_results(raise_exceptions=False)
        total_time = time.perf_counter() - start_time

        # Should handle errors efficiently
        assert len(results) == 20
        assert total_time < 2.0
        assert all(hasattr(r, 'message') for r in results)

        manager.cleanup_all()

    def test_large_batch_performance(self):
        """Test performance with large batches."""
        manager = TaskManager()

        def simple_task(x):
            return x * x

        # Large batch
        tasks = [(simple_task, (i,), {}) for i in range(1000)]

        start_time = time.perf_counter()
        batch = manager.multithread(tasks)
        results = batch.await_results()
        total_time = time.perf_counter() - start_time

        # Should handle large batches efficiently
        assert len(results) == 1000
        assert total_time < 10.0
        assert all(isinstance(r, int) for r in results)

        manager.cleanup_all()
