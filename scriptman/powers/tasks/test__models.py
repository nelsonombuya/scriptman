"""
Unit tests for TaskException, Task, and Tasks classes.
"""
import pickle
import time
from concurrent.futures import Future
from unittest.mock import Mock

import pytest

from scriptman.powers.tasks._models import Task, TaskException, Tasks


class TestTaskException:
    """Test cases for TaskException class."""

    def test_exception_creation_with_message_only(self):
        """Test TaskException creation with only a message."""
        exc = TaskException("Test error message")

        assert exc.message == "Test error message"
        assert isinstance(exc.exception, Exception)
        assert str(exc.exception) == "Test error message"
        assert len(exc.stacktrace) > 0

    def test_exception_creation_with_original_exception(self):
        """Test TaskException creation with original exception."""
        original = ValueError("Original error")
        exc = TaskException("Wrapper message", original)

        assert exc.message == "Wrapper message"
        assert exc.exception is original
        assert len(exc.stacktrace) > 0

    def test_exception_creation_with_none_exception(self):
        """Test TaskException creation with None exception."""
        exc = TaskException("Test message", None)

        assert exc.message == "Test message"
        assert isinstance(exc.exception, Exception)
        assert str(exc.exception) == "Test message"

    def test_stacktrace_generation(self):
        """Test stacktrace generation structure."""
        exc = TaskException("Test error")

        assert isinstance(exc.stacktrace, list)
        assert len(exc.stacktrace) > 0

        # Check stacktrace structure
        frame = exc.stacktrace[0]
        assert "frame" in frame
        assert "file" in frame
        assert "line" in frame
        assert "function" in frame
        assert "code" in frame

        assert isinstance(frame["frame"], int)
        assert isinstance(frame["file"], str)
        assert isinstance(frame["line"], int)
        assert isinstance(frame["function"], str)

    def test_to_dict_property(self):
        """Test to_dict property serialization."""
        original = RuntimeError("Original error")
        exc = TaskException("Wrapper message", original)

        result = exc.to_dict

        assert isinstance(result, dict)
        assert result["message"] == "Wrapper message"
        assert result["exception"]["type"] == "RuntimeError"
        assert result["exception"]["message"] == "Original error"
        assert isinstance(result["stacktrace"], list)

    def test_to_dict_with_none_exception(self):
        """Test to_dict with None exception."""
        exc = TaskException("Test message", None)
        result = exc.to_dict

        assert result["message"] == "Test message"
        assert result["exception"]["type"] is None
        assert result["exception"]["message"] is None

    def test_string_representation(self):
        """Test string representation."""
        original = ValueError("Original error")
        exc = TaskException("Wrapper message", original)

        str_repr = str(exc)
        assert str_repr == "ValueError: Wrapper message"

    def test_string_representation_with_none_exception(self):
        """Test string representation with None exception."""
        exc = TaskException("Test message", None)
        str_repr = str(exc)
        assert str_repr == "Exception: Test message"

    def test_pickling_support(self):
        """Test pickling and unpickling support."""
        original = KeyError("Key not found")
        exc = TaskException("Pickle test", original)

        # Pickle the exception
        pickled = pickle.dumps(exc)

        # Unpickle it
        unpickled = pickle.loads(pickled)

        assert unpickled.message == "Pickle test"
        assert isinstance(unpickled.exception, KeyError)
        assert str(unpickled.exception) == "Key not found"
        assert len(unpickled.stacktrace) > 0

    def test_pickling_with_none_exception(self):
        """Test pickling with None exception."""
        exc = TaskException("Pickle test", None)

        pickled = pickle.dumps(exc)
        unpickled = pickle.loads(pickled)

        assert unpickled.message == "Pickle test"
        assert isinstance(unpickled.exception, Exception)
        assert str(unpickled.exception) == "Pickle test"

    def test_setstate_with_none_state(self):
        """Test __setstate__ with None state."""
        exc = TaskException("Test")
        exc.__setstate__(None)

        assert exc.message == ""
        assert exc.stacktrace == []
        assert isinstance(exc.exception, Exception)

    def test_setstate_with_empty_state(self):
        """Test __setstate__ with empty state."""
        exc = TaskException("Test")
        exc.__setstate__({})

        assert exc.message == ""
        assert exc.stacktrace == []
        assert isinstance(exc.exception, Exception)

    def test_setstate_with_partial_state(self):
        """Test __setstate__ with partial state."""
        exc = TaskException("Test")
        state = {"message": "Restored message"}
        exc.__setstate__(state)

        assert exc.message == "Restored message"
        assert exc.stacktrace == []
        assert isinstance(exc.exception, Exception)

    def test_inheritance_from_exception(self):
        """Test that TaskException properly inherits from Exception."""
        exc = TaskException("Test error")

        assert isinstance(exc, Exception)
        assert issubclass(TaskException, Exception)

    def test_exception_in_except_clause(self):
        """Test TaskException can be caught in except clauses."""
        try:
            raise TaskException("Test error")
        except TaskException as e:
            assert e.message == "Test error"
        except Exception:
            pytest.fail("TaskException should be catchable as Exception")

    def test_nested_exception_handling(self):
        """Test TaskException with nested exception handling."""
        try:
            try:
                raise ValueError("Inner error")
            except ValueError as inner:
                raise TaskException("Outer wrapper", inner)
        except TaskException as outer:
            assert outer.message == "Outer wrapper"
            assert isinstance(outer.exception, ValueError)
            assert str(outer.exception) == "Inner error"


class TestTask:
    """Test cases for Task class."""

    def test_task_creation_with_future(self, completed_future):
        """Test Task creation with a Future."""
        task = Task(completed_future)

        assert task.future is completed_future
        assert task.start_time > 0
        assert isinstance(task.start_time, float)

    def test_task_creation_with_default_start_time(self, completed_future):
        """Test Task creation uses current time as start_time."""
        before = time.perf_counter()
        task = Task(completed_future)
        after = time.perf_counter()

        assert before <= task.start_time <= after

    def test_await_result_success(self, completed_future):
        """Test await_result with successful completion."""
        task = Task(completed_future)

        result = task.await_result()
        assert result == "test_result"

    def test_await_result_success_with_timeout(self, completed_future):
        """Test await_result with timeout on completed task."""
        task = Task(completed_future)

        result = task.await_result(timeout=1.0)
        assert result == "test_result"

    def test_await_result_failure_raises_exception(self, failed_future):
        """Test await_result raises exception when raise_exceptions=True."""
        task = Task(failed_future)

        with pytest.raises(ValueError, match="test_error"):
            task.await_result(raise_exceptions=True)

    def test_await_result_failure_returns_task_exception(self, failed_future):
        """Test await_result returns TaskException when raise_exceptions=False."""
        task = Task(failed_future)

        result = task.await_result(raise_exceptions=False)
        assert isinstance(result, TaskException)
        assert result.message == "test_error"
        assert isinstance(result.exception, ValueError)

    def test_await_result_timeout(self, pending_future):
        """Test await_result with timeout on pending task."""
        task = Task(pending_future)

        with pytest.raises(Exception):  # TimeoutError or similar
            task.await_result(timeout=0.01)

    def test_is_done_property(self, completed_future, failed_future, pending_future):
        """Test is_done property."""
        completed_task = Task(completed_future)
        failed_task = Task(failed_future)
        pending_task = Task(pending_future)

        assert completed_task.is_done is True
        assert failed_task.is_done is True
        assert pending_task.is_done is False

    def test_is_successful_property(self, completed_future, failed_future, pending_future):
        """Test is_successful property."""
        completed_task = Task(completed_future)
        failed_task = Task(failed_future)
        pending_task = Task(pending_future)

        assert completed_task.is_successful is True
        assert failed_task.is_successful is False
        assert pending_task.is_successful is False

    def test_is_successful_with_cancelled_future(self):
        """Test is_successful with cancelled future."""
        future = Future()
        future.cancel()
        task = Task(future)

        assert task.is_successful is False

    def test_duration_property(self, completed_future):
        """Test duration property."""
        task = Task(completed_future)

        # Duration should be small since future is already completed
        assert task.duration >= 0
        assert isinstance(task.duration, float)

    def test_duration_with_slow_task(self):
        """Test duration property with slow task."""
        future = Future()
        task = Task(future)

        # Simulate some time passing
        time.sleep(0.01)

        duration = task.duration
        assert duration >= 0.01
        assert duration < 0.1  # Should be reasonable

    def test_exception_property(self, failed_future, completed_future, pending_future):
        """Test exception property."""
        failed_task = Task(failed_future)
        completed_task = Task(completed_future)
        pending_task = Task(pending_future)

        assert isinstance(failed_task.exception, ValueError)
        assert completed_task.exception is None
        assert pending_task.exception is None

    def test_exception_property_with_cancelled_future(self):
        """Test exception property with cancelled future."""
        future = Future()
        future.cancel()
        task = Task(future)

        assert task.exception is None

    def test_string_representation_completed(self, completed_future):
        """Test string representation for completed task."""
        task = Task(completed_future)

        str_repr = str(task)
        assert "Task(completed" in str_repr
        assert "duration=" in str_repr

    def test_string_representation_running(self, pending_future):
        """Test string representation for running task."""
        task = Task(pending_future)

        str_repr = str(task)
        assert "Task(running" in str_repr
        assert "duration=" in str_repr

    def test_generic_type_hint(self):
        """Test Task with generic type hint."""
        future = Future[str]()
        future.set_result("string_result")
        task = Task[str](future)

        result = task.await_result()
        assert isinstance(result, str)
        assert result == "string_result"

    def test_task_with_different_result_types(self):
        """Test Task with different result types."""
        # Test with int
        int_future = Future[int]()
        int_future.set_result(42)
        int_task = Task[int](int_future)
        assert int_task.await_result() == 42

        # Test with list
        list_future = Future[list]()
        list_future.set_result([1, 2, 3])
        list_task = Task[list](list_future)
        assert list_task.await_result() == [1, 2, 3]

        # Test with dict
        dict_future = Future[dict]()
        dict_future.set_result({"key": "value"})
        dict_task = Task[dict](dict_future)
        assert dict_task.await_result() == {"key": "value"}

    def test_task_with_none_result(self):
        """Test Task with None result."""
        future = Future()
        future.set_result(None)
        task = Task(future)

        result = task.await_result()
        assert result is None

    def test_task_with_complex_exception(self):
        """Test Task with complex exception."""
        future = Future()
        complex_exc = RuntimeError("Complex error with details")
        future.set_exception(complex_exc)
        task = Task(future)

        with pytest.raises(RuntimeError, match="Complex error with details"):
            task.await_result(raise_exceptions=True)

        result = task.await_result(raise_exceptions=False)
        assert isinstance(result, TaskException)
        assert isinstance(result.exception, RuntimeError)

    def test_task_start_time_accuracy(self):
        """Test that start_time is accurate."""
        before = time.perf_counter()
        future = Future()
        task = Task(future)
        after = time.perf_counter()

        assert before <= task.start_time <= after
        assert abs(task.start_time - before) < 0.001  # Very small tolerance

    def test_task_with_multiple_await_calls(self, completed_future):
        """Test multiple await_result calls on same task."""
        task = Task(completed_future)

        result1 = task.await_result()
        result2 = task.await_result()

        assert result1 == result2 == "test_result"

    def test_task_properties_consistency(self, completed_future, failed_future):
        """Test that task properties are consistent."""
        completed_task = Task(completed_future)
        failed_task = Task(failed_future)

        # Completed task should be done and successful
        assert completed_task.is_done is True
        assert completed_task.is_successful is True
        assert completed_task.exception is None

        # Failed task should be done but not successful
        assert failed_task.is_done is True
        assert failed_task.is_successful is False
        assert failed_task.exception is not None


class TestTasks:
    """Test cases for Tasks class."""

    def test_empty_tasks_collection(self):
        """Test empty Tasks collection."""
        tasks = Tasks()

        assert len(tasks) == 0
        assert tasks.total_count == 0
        assert tasks.completed_count == 0
        assert tasks.are_successful is True  # Empty collection is considered successful
        assert list(tasks.running_tasks) == []
        assert list(tasks.completed_tasks) == []
        assert list(tasks.successful_tasks) == []

    def test_tasks_with_single_task(self, completed_future):
        """Test Tasks with single task."""
        task = Task(completed_future)
        tasks = Tasks([task])

        assert len(tasks) == 1
        assert tasks.total_count == 1
        assert tasks.completed_count == 1
        assert tasks.are_successful is True
        assert len(tasks.running_tasks) == 0
        assert len(tasks.completed_tasks) == 1
        assert len(tasks.successful_tasks) == 1

    def test_tasks_with_multiple_tasks(self):
        """Test Tasks with multiple tasks."""
        futures = [Future() for _ in range(3)]
        for i, future in enumerate(futures):
            future.set_result(f"result_{i}")

        tasks = Tasks([Task(f) for f in futures])

        assert len(tasks) == 3
        assert tasks.total_count == 3
        assert tasks.completed_count == 3
        assert tasks.are_successful is True

    def test_tasks_with_mixed_success_failure(self):
        """Test Tasks with mixed success and failure."""
        success_future = Future()
        success_future.set_result("success")

        fail_future = Future()
        fail_future.set_exception(ValueError("failure"))

        tasks = Tasks([Task(success_future), Task(fail_future)])

        assert len(tasks) == 2
        assert tasks.completed_count == 2
        assert tasks.are_successful is False
        assert len(tasks.successful_tasks) == 1
        assert len(tasks.completed_tasks) == 2

    def test_await_results_eager_success(self):
        """Test await_results in eager mode with successful tasks."""
        futures = [Future() for _ in range(3)]
        for i, future in enumerate(futures):
            future.set_result(f"result_{i}")

        tasks = Tasks([Task(f) for f in futures])

        results = tasks.await_results()
        assert results == ["result_0", "result_1", "result_2"]

    def test_await_results_eager_with_failures_raise_exceptions(self):
        """Test await_results eager mode raises exceptions."""
        success_future = Future()
        success_future.set_result("success")

        fail_future = Future()
        fail_future.set_exception(ValueError("failure"))

        tasks = Tasks([Task(success_future), Task(fail_future)])

        with pytest.raises(ValueError, match="failure"):
            tasks.await_results(raise_exceptions=True)

    def test_await_results_eager_with_failures_return_exceptions(self):
        """Test await_results eager mode returns TaskException objects."""
        success_future = Future()
        success_future.set_result("success")

        fail_future = Future()
        fail_future.set_exception(ValueError("failure"))

        tasks = Tasks([Task(success_future), Task(fail_future)])

        results = tasks.await_results(raise_exceptions=False)
        assert len(results) == 2
        assert results[0] == "success"
        assert isinstance(results[1], TaskException)

    def test_await_results_eager_only_successful_results(self):
        """Test await_results eager mode with only_successful_results=True."""
        success_future = Future()
        success_future.set_result("success")

        fail_future = Future()
        fail_future.set_exception(ValueError("failure"))

        tasks = Tasks([Task(success_future), Task(fail_future)])

        results = tasks.await_results(only_successful_results=True, raise_exceptions=False)
        assert results == ["success"]

    def test_await_results_lazy_success(self):
        """Test await_results in lazy mode with successful tasks."""
        futures = [Future() for _ in range(3)]
        for i, future in enumerate(futures):
            future.set_result(f"result_{i}")

        tasks = Tasks([Task(f) for f in futures])

        results = list(tasks.await_results(lazy=True))
        assert len(results) == 3
        assert set(results) == {"result_0", "result_1", "result_2"}

    def test_await_results_lazy_with_failures_raise_exceptions(self):
        """Test await_results lazy mode raises exceptions."""
        success_future = Future()
        success_future.set_result("success")

        fail_future = Future()
        fail_future.set_exception(ValueError("failure"))

        tasks = Tasks([Task(success_future), Task(fail_future)])

        with pytest.raises(ValueError, match="failure"):
            list(tasks.await_results(lazy=True, raise_exceptions=True))

    def test_await_results_lazy_with_failures_return_exceptions(self):
        """Test await_results lazy mode returns TaskException objects."""
        success_future = Future()
        success_future.set_result("success")

        fail_future = Future()
        fail_future.set_exception(ValueError("failure"))

        tasks = Tasks([Task(success_future), Task(fail_future)])

        results = list(tasks.await_results(lazy=True, raise_exceptions=False))
        assert len(results) == 2
        assert "success" in results
        assert any(isinstance(r, TaskException) for r in results)

    def test_await_results_lazy_only_successful_results(self):
        """Test await_results lazy mode with only_successful_results=True."""
        success_future = Future()
        success_future.set_result("success")

        fail_future = Future()
        fail_future.set_exception(ValueError("failure"))

        tasks = Tasks([Task(success_future), Task(fail_future)])

        results = list(tasks.await_results(lazy=True, only_successful_results=True, raise_exceptions=False))
        assert results == ["success"]

    def test_await_results_with_timeout(self):
        """Test await_results with timeout."""
        # Create one completed and one pending task
        completed_future = Future()
        completed_future.set_result("completed")

        pending_future = Future()

        tasks = Tasks([Task(completed_future), Task(pending_future)])

        with pytest.raises(Exception):  # TimeoutError or similar
            tasks.await_results(timeout=0.01)

    def test_collection_operations(self):
        """Test collection operations (__len__, __iter__, __getitem__)."""
        futures = [Future() for _ in range(3)]
        for i, future in enumerate(futures):
            future.set_result(f"result_{i}")

        tasks = Tasks([Task(f) for f in futures])

        # Test __len__
        assert len(tasks) == 3

        # Test __iter__
        task_list = list(tasks)
        assert len(task_list) == 3
        assert all(isinstance(task, Task) for task in task_list)

        # Test __getitem__
        assert isinstance(tasks[0], Task)
        assert isinstance(tasks[1], Task)
        assert isinstance(tasks[2], Task)

        with pytest.raises(IndexError):
            _ = tasks[3]

    def test_string_representation(self):
        """Test string representation."""
        futures = [Future() for _ in range(3)]
        for future in futures:
            future.set_result("result")

        tasks = Tasks([Task(f) for f in futures])

        str_repr = str(tasks)
        assert "Tasks(3/3 completed)" in str_repr

    def test_string_representation_partial_completion(self):
        """Test string representation with partial completion."""
        completed_future = Future()
        completed_future.set_result("result")

        pending_future = Future()

        tasks = Tasks([Task(completed_future), Task(pending_future)])

        str_repr = str(tasks)
        assert "Tasks(1/2 completed)" in str_repr

    def test_running_tasks_property(self):
        """Test running_tasks property."""
        completed_future = Future()
        completed_future.set_result("result")

        pending_future = Future()

        tasks = Tasks([Task(completed_future), Task(pending_future)])

        running = tasks.running_tasks
        assert len(running) == 1
        assert running[0].future is pending_future

    def test_completed_tasks_property(self):
        """Test completed_tasks property."""
        completed_future = Future()
        completed_future.set_result("result")

        pending_future = Future()

        tasks = Tasks([Task(completed_future), Task(pending_future)])

        completed = tasks.completed_tasks
        assert len(completed) == 1
        assert completed[0].future is completed_future

    def test_successful_tasks_property(self):
        """Test successful_tasks property."""
        success_future = Future()
        success_future.set_result("success")

        fail_future = Future()
        fail_future.set_exception(ValueError("failure"))

        tasks = Tasks([Task(success_future), Task(fail_future)])

        successful = tasks.successful_tasks
        assert len(successful) == 1
        assert successful[0].future is success_future

    def test_tasks_with_generic_types(self):
        """Test Tasks with generic types."""
        int_future = Future[int]()
        int_future.set_result(42)

        str_future = Future[str]()
        str_future.set_result("hello")

        tasks = Tasks[int]([Task(int_future)])
        results = tasks.await_results()
        assert results == [42]

    def test_empty_tasks_await_results(self):
        """Test await_results on empty Tasks collection."""
        tasks = Tasks()

        results = tasks.await_results()
        assert results == []

        lazy_results = list(tasks.await_results(lazy=True))
        assert lazy_results == []

    def test_tasks_with_cancelled_futures(self):
        """Test Tasks with cancelled futures."""
        cancelled_future = Future()
        cancelled_future.cancel()

        success_future = Future()
        success_future.set_result("success")

        tasks = Tasks([Task(cancelled_future), Task(success_future)])

        assert tasks.completed_count == 2
        assert tasks.are_successful is False
        assert len(tasks.successful_tasks) == 1

    def test_await_results_lazy_iterator_behavior(self):
        """Test that lazy await_results returns an iterator."""
        futures = [Future() for _ in range(2)]
        for i, future in enumerate(futures):
            future.set_result(f"result_{i}")

        tasks = Tasks([Task(f) for f in futures])

        iterator = tasks.await_results(lazy=True)
        assert hasattr(iterator, '__iter__')
        assert hasattr(iterator, '__next__')

        results = list(iterator)
        assert len(results) == 2
