"""
Unit tests for ExecutionManager ABC.
"""
import pytest
from abc import ABC

from scriptman.powers.tasks._execution_manager import ExecutionManager


class TestExecutionManager:
    """Test cases for ExecutionManager ABC."""

    def test_cannot_instantiate_abc_directly(self):
        """Test that ExecutionManager cannot be instantiated directly."""
        with pytest.raises(TypeError):
            ExecutionManager()

    def test_is_abstract_base_class(self):
        """Test that ExecutionManager is an abstract base class."""
        assert issubclass(ExecutionManager, ABC)

    def test_has_required_abstract_methods(self):
        """Test that ExecutionManager has all required abstract methods."""
        abstract_methods = ExecutionManager.__abstractmethods__

        required_methods = {
            'submit_task',
            'cleanup',
            'is_shutdown',
            'is_idle',
            'last_activity_time',
            'update_resource_limits'
        }

        assert abstract_methods == required_methods

    def test_subclass_must_implement_all_methods(self):
        """Test that subclass must implement all abstract methods."""

        class IncompleteExecutor(ExecutionManager):
            def submit_task(self, func, *args, **kwargs):
                pass

            def cleanup(self, wait=True, timeout=None):
                pass

            # Missing other required methods

        with pytest.raises(TypeError):
            IncompleteExecutor()

    def test_complete_subclass_can_be_instantiated(self):
        """Test that complete subclass can be instantiated."""

        class CompleteExecutor(ExecutionManager):
            def submit_task(self, func, *args, **kwargs):
                pass

            def cleanup(self, wait=True, timeout=None):
                pass

            @property
            def is_shutdown(self):
                return False

            @property
            def is_idle(self):
                return True

            @property
            def last_activity_time(self):
                return 0.0

            def update_resource_limits(self, max_workers):
                pass

        # Should not raise any exception
        executor = CompleteExecutor()
        assert executor is not None

    def test_abstract_methods_have_correct_signatures(self):
        """Test that abstract methods have correct signatures."""
        import inspect

        # Check submit_task signature
        submit_sig = inspect.signature(ExecutionManager.submit_task)
        assert 'func' in submit_sig.parameters
        assert 'args' in submit_sig.parameters
        assert 'kwargs' in submit_sig.parameters

        # Check cleanup signature
        cleanup_sig = inspect.signature(ExecutionManager.cleanup)
        assert 'wait' in cleanup_sig.parameters
        assert 'timeout' in cleanup_sig.parameters

        # Check update_resource_limits signature
        update_sig = inspect.signature(ExecutionManager.update_resource_limits)
        assert 'max_workers' in update_sig.parameters

    def test_property_methods_are_abstract(self):
        """Test that property methods are properly abstract."""
        # These should be properties, not regular methods
        assert isinstance(ExecutionManager.is_shutdown, property)
        assert isinstance(ExecutionManager.is_idle, property)
        assert isinstance(ExecutionManager.last_activity_time, property)

    def test_abstract_methods_have_docstrings(self):
        """Test that abstract methods have docstrings."""
        assert ExecutionManager.submit_task.__doc__ is not None
        assert ExecutionManager.cleanup.__doc__ is not None
        assert ExecutionManager.is_shutdown.fget.__doc__ is not None
        assert ExecutionManager.is_idle.fget.__doc__ is not None
        assert ExecutionManager.last_activity_time.fget.__doc__ is not None
        assert ExecutionManager.update_resource_limits.__doc__ is not None

    def test_abstract_methods_are_callable(self):
        """Test that abstract methods are callable (for signature inspection)."""
        assert callable(ExecutionManager.submit_task)
        assert callable(ExecutionManager.cleanup)
        assert callable(ExecutionManager.update_resource_limits)

        # Properties are callable through their fget
        assert callable(ExecutionManager.is_shutdown.fget)
        assert callable(ExecutionManager.is_idle.fget)
        assert callable(ExecutionManager.last_activity_time.fget)
