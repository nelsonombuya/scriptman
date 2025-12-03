from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    ParamSpec,
    TypeVar,
)

# ⚙️ Base type variables used across orchestrator domains
P = ParamSpec("P")
R = TypeVar("R")
C = TypeVar("C")

# 🧠 Tightly typed callable aliases preserving signature information
TypedSyncFunc = Callable[P, R]
"""🧠 Synchronous callable with tightly typed signature.

Uses ParamSpec and TypeVar to preserve type information through decorators
and higher-order functions. Default choice for synchronous callables in Scriptman.

Prefer over UntypedCallable when signature preservation matters.
"""

TypedAsyncFunc = Callable[P, Awaitable[R]]
"""🧠 Asynchronous callable with tightly typed signature.

Uses ParamSpec and TypeVar to preserve type information for async functions.
Maintains type relationships through awaitable results.

Prefer over UntypedCallable when signature preservation matters.
"""

SyncGenFunc = Callable[P, Generator[R, Any, Any]]
"""🧠 Synchronous generator callable with tightly typed signature.

Preserves type information for generators that yield typed values.
Use when you need lazy evaluation with type safety.
"""

AsyncGenFunc = Callable[P, AsyncGenerator[R, Any]]
"""🧠 Asynchronous generator callable with tightly typed signature.

Preserves type information for async generators that yield typed values.
Use when you need async lazy evaluation with type safety.
"""

TypedFunc = Callable[P, R | Awaitable[R]]
"""🧠 Tightly typed callable preserving signature information.

Uses ParamSpec and TypeVar to maintain type information through
decorators and higher-order functions. Default choice for most
callable types in Scriptman.

Prefer over UntypedCallable when signature preservation matters.
"""

GenFunc = Callable[P, Generator[R, Any, Any] | AsyncGenerator[R, Any]]
"""🧠 Generator callable (sync or async) with tightly typed signature.

Covers both synchronous and asynchronous generators while preserving
type information for yielded values.
"""

TypedAnyFunc = Callable[
    P, R | Awaitable[R] | Generator[R, Any, Any] | AsyncGenerator[R, Any]
]
"""🧠 Most flexible tightly typed callable including generators.

Covers all callable types (sync, async, sync generators, async generators)
while still preserving signature information through ParamSpec.

Use when you need maximum flexibility but want to maintain type relationships.
"""

# 🌉 Context-aware callables
ContextFunc = Callable[[C], R | Awaitable[R]]
"""🌉 Context-aware callable with tightly typed signature.

Takes a context object as the first argument and returns a typed result.
Preserves type information for both the context and return type.

Use for callables that require runtime context (e.g., services, schedules).
"""

# 🛠️ Loosely typed callable helpers (no signature preservation)
UntypedCallable = Callable[..., Awaitable[Any] | Any]
"""🛠️ Loosely typed callable without signature preservation.

Uses ellipsis (...) for arguments, allowing any callable signature.
Provides maximum flexibility at the cost of type information.

Use only when you truly need to accept any callable type and cannot
preserve signature information (e.g., TaskEntry.target for flexibility).
"""

UntypedContextCallable = Callable[[C], Awaitable[Any] | Any]
"""🛠️ Loosely typed context-aware callable without signature preservation.

Takes a context object as the first argument but accepts any return type.
Provides flexibility while maintaining the context contract.

Use when context is required but return type varies widely.
"""

__all__ = [
    "P",
    "R",
    "C",
    "TypedSyncFunc",
    "TypedAsyncFunc",
    "SyncGenFunc",
    "AsyncGenFunc",
    "TypedFunc",
    "GenFunc",
    "TypedAnyFunc",
    "ContextFunc",
    "UntypedCallable",
    "UntypedContextCallable",
]
