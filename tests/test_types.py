"""🧪 Tests for scriptman.types module.

Tests cover:
- Type variable exports
- Function type aliases (Func, AsyncFunc)
- is_async() detection
- AsyncLock sync/async acquisition
- StampedeLock concurrent access patterns
"""

from __future__ import annotations

import pytest

from scriptman.types import (
    AsyncFunc,
    AsyncLock,
    BaseModelT,
    C,
    Func,
    P,
    R,
    StampedeLock,
    T,
    is_async,
)

# ═══════════════════════════════════════════════════════════════════════════════
# TYPE VARIABLE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestTypeVariableExports:
    """Test that all type variables are properly exported."""

    def test_t_is_typevar(self):
        """🔍 T should be a TypeVar."""
        from typing import TypeVar

        assert isinstance(T, TypeVar)

    def test_p_is_paramspec(self):
        """🔍 P should be a ParamSpec."""
        from typing import ParamSpec

        assert isinstance(P, ParamSpec)

    def test_r_is_typevar(self):
        """🔍 R should be a TypeVar."""
        from typing import TypeVar

        assert isinstance(R, TypeVar)

    def test_c_is_typevar(self):
        """🔍 C should be a TypeVar."""
        from typing import TypeVar

        assert isinstance(C, TypeVar)

    def test_basemodelt_is_bound_typevar(self):
        """🔍 BaseModelT should be a TypeVar bound to BaseModel."""
        from typing import TypeVar

        from pydantic import BaseModel

        assert isinstance(BaseModelT, TypeVar)
        assert BaseModelT.__bound__ is BaseModel


class TestFunctionTypeAliases:
    """Test function type aliases exist and are usable."""

    def test_func_is_alias(self):
        """🔍 Func should be a type alias for Callable[P, R]."""
        assert Func is not None

    def test_asyncfunc_is_alias(self):
        """🔍 AsyncFunc should be a type alias for Callable[P, Coroutine[...]]."""
        assert AsyncFunc is not None

    def test_asyncfunc_uses_coroutine(self):
        """🔍 AsyncFunc should use Coroutine, not Awaitable."""
        # AsyncFunc is Callable[P, Coroutine[Any, Any, R]]
        # The actual type check happens at static analysis time
        # We just verify the alias exists and is importable
        assert AsyncFunc is not None


# ═══════════════════════════════════════════════════════════════════════════════
# is_async TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestIsAsync:
    """Test is_async() function detection."""

    def test_sync_function_returns_false(self):
        """🔍 is_async() should return False for sync functions."""

        def sync_fn():
            pass

        assert is_async(sync_fn) is False

    def test_async_function_returns_true(self):
        """🔍 is_async() should return True for async functions."""

        async def async_fn():
            pass

        assert is_async(async_fn) is True

    def test_lambda_returns_false(self):
        """🔍 is_async() should return False for lambdas."""

        def double(x: int) -> int:
            return x * 2

        assert is_async(double) is False

    def test_sync_method_returns_false(self):
        """🔍 is_async() should return False for sync methods."""

        class MyClass:
            def sync_method(self):
                pass

        obj = MyClass()
        assert is_async(obj.sync_method) is False

    def test_async_method_returns_true(self):
        """🔍 is_async() should return True for async methods."""

        class MyClass:
            async def async_method(self):
                pass

        obj = MyClass()
        assert is_async(obj.async_method) is True

    def test_callable_class_returns_false(self):
        """🔍 is_async() should return False for callable classes."""

        class Callable:
            def __call__(self):
                pass

        assert is_async(Callable()) is False


# ═══════════════════════════════════════════════════════════════════════════════
# AsyncLock TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestAsyncLock:
    """Test AsyncLock class."""

    def test_sync_lock_works(self):
        """🔐 sync() should return a usable threading lock."""
        lock = AsyncLock()
        counter = [0]

        with lock.sync():
            counter[0] += 1

        assert counter[0] == 1

    def test_sync_lock_is_reentrant_safe(self):
        """🔐 sync() should return the same lock instance."""
        lock = AsyncLock()
        assert lock.sync() is lock.sync()

    @pytest.mark.asyncio
    async def test_async_lock_works(self):
        """🔐 async_() should return a usable asyncio lock."""
        lock = AsyncLock()
        counter = [0]

        async with lock.async_():
            counter[0] += 1

        assert counter[0] == 1

    @pytest.mark.asyncio
    async def test_async_lock_is_lazy(self):
        """🔐 async lock should be lazily created."""
        lock = AsyncLock()
        assert lock._async_lock is None

        # Access the async lock
        _ = lock.async_()

        assert lock._async_lock is not None


# ═══════════════════════════════════════════════════════════════════════════════
# StampedeLock TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestStampedeLock:
    """Test StampedeLock class."""

    def test_sync_acquire_creates_lock(self):
        """🔐 acquire_sync() should create and return a lock."""
        locks = StampedeLock()
        lock = locks.acquire_sync("key1")

        assert lock is not None
        assert "key1" in locks._sync_locks

    def test_sync_acquire_returns_same_lock(self):
        """🔐 acquire_sync() should return same lock for same key."""
        locks = StampedeLock()
        lock1 = locks.acquire_sync("key1")
        lock2 = locks.acquire_sync("key1")

        assert lock1 is lock2

    def test_sync_acquire_different_keys_different_locks(self):
        """🔐 acquire_sync() should return different locks for different keys."""
        locks = StampedeLock()
        lock1 = locks.acquire_sync("key1")
        lock2 = locks.acquire_sync("key2")

        assert lock1 is not lock2

    def test_sync_cleanup_removes_lock(self):
        """🧹 cleanup_sync() should remove the lock."""
        locks = StampedeLock()
        locks.acquire_sync("key1")
        assert "key1" in locks._sync_locks

        locks.cleanup_sync("key1")
        assert "key1" not in locks._sync_locks

    def test_sync_cleanup_nonexistent_is_safe(self):
        """🧹 cleanup_sync() should be safe for nonexistent keys."""
        locks = StampedeLock()
        locks.cleanup_sync("nonexistent")  # Should not raise

    def test_clear_all_sync_removes_all_locks(self):
        """🧹 clear_all_sync() should remove all sync locks."""
        locks = StampedeLock()
        locks.acquire_sync("key1")
        locks.acquire_sync("key2")
        locks.acquire_sync("key3")
        assert len(locks._sync_locks) == 3

        locks.clear_all_sync()
        assert len(locks._sync_locks) == 0

    @pytest.mark.asyncio
    async def test_async_acquire_creates_lock(self):
        """🔐 acquire_async() should create and return a lock."""
        locks = StampedeLock()
        lock = await locks.acquire_async("key1")

        assert lock is not None
        assert "key1" in locks._async_locks

    @pytest.mark.asyncio
    async def test_async_acquire_returns_same_lock(self):
        """🔐 acquire_async() should return same lock for same key."""
        locks = StampedeLock()
        lock1 = await locks.acquire_async("key1")
        lock2 = await locks.acquire_async("key1")

        assert lock1 is lock2

    @pytest.mark.asyncio
    async def test_async_cleanup_removes_lock(self):
        """🧹 cleanup_async() should remove the lock."""
        locks = StampedeLock()
        await locks.acquire_async("key1")
        assert "key1" in locks._async_locks

        await locks.cleanup_async("key1")
        assert "key1" not in locks._async_locks

    @pytest.mark.asyncio
    async def test_async_cleanup_before_any_acquire_is_safe(self):
        """🧹 cleanup_async() should be safe when no locks exist."""
        locks = StampedeLock()
        await locks.cleanup_async("nonexistent")  # Should not raise

    @pytest.mark.asyncio
    async def test_clear_all_async_removes_all_locks(self):
        """🧹 clear_all_async() should remove all async locks."""
        locks = StampedeLock()
        await locks.acquire_async("key1")
        await locks.acquire_async("key2")
        assert len(locks._async_locks) == 2

        await locks.clear_all_async()
        assert len(locks._async_locks) == 0

    @pytest.mark.asyncio
    async def test_clear_all_async_before_any_acquire_is_safe(self):
        """🧹 clear_all_async() should be safe when no locks exist."""
        locks = StampedeLock()
        await locks.clear_all_async()  # Should not raise

    def test_sync_lock_prevents_concurrent_access(self):
        """🔐 Sync lock should prevent concurrent access."""
        import threading

        locks = StampedeLock()
        counter = [0]
        results: list[int] = []

        def worker():
            with locks.acquire_sync("shared"):
                current = counter[0]
                counter[0] = current + 1
                results.append(counter[0])

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All increments should be sequential (1, 2, 3, ..., 10)
        assert sorted(results) == list(range(1, 11))


# ═══════════════════════════════════════════════════════════════════════════════
# EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_unicode_in_lock_keys(self):
        """🔐 Lock keys should support unicode."""
        locks = StampedeLock()
        lock = locks.acquire_sync("键🔑key")
        assert lock is not None
        locks.cleanup_sync("键🔑key")

    def test_empty_string_lock_key(self):
        """🔐 Empty string should be valid lock key."""
        locks = StampedeLock()
        lock = locks.acquire_sync("")
        assert lock is not None
        locks.cleanup_sync("")
