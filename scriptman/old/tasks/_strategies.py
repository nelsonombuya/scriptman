from __future__ import annotations

from typing import MutableMapping

from scriptman.orchestrator._context import ExecutorRegistry, ExecutorStrategy


class InlineExecutorStrategy(ExecutorStrategy):
    name = "inline"

    def acquire(self) -> ExecutorStrategy:
        """🔄 Acquire the inline executor strategy."""
        return self

    def release(self) -> None:  # pragma: no cover - no-op
        """🔄 Release the inline executor strategy."""
        return None


class InlineExecutorRegistry(ExecutorRegistry):
    def __init__(self) -> None:
        """🔄 Initialize the inline executor registry."""
        self._strategies: MutableMapping[str, ExecutorStrategy] = {
            "inline": InlineExecutorStrategy()
        }

    def get(self, name: str) -> ExecutorStrategy:
        """🔄 Get the inline executor strategy."""
        try:
            return self._strategies[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise RuntimeError(f"⚠️ Unknown executor strategy '{name}'") from exc

    def register(self, strategy: ExecutorStrategy) -> None:
        """🔄 Register the inline executor strategy."""
        self._strategies[strategy.name] = strategy
