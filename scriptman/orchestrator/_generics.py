from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    ParamSpec,
    TypeVar,
    Union,
)

# ⚙ Type variables for generic types
T = TypeVar("T")  # For generic argument or return types
P = ParamSpec("P")  # For generic argument types
R = TypeVar("R")  # For generic return types (if both T and R are used)
C = TypeVar("C")  # Context parameter type (single-argument callables)

# For synchronous functions
SyncFunc = Callable[P, R]
SyncGenFunc = Callable[P, Generator[R, Any, Any]]

# For asynchronous functions
AsyncFunc = Callable[P, Awaitable[R]]
AsyncGenFunc = Callable[P, AsyncGenerator[R, Any]]

# For both sync and async functions
Func = Union[SyncFunc[P, R], AsyncFunc[P, R]]
GenFunc = Union[SyncGenFunc[P, R], AsyncGenFunc[P, R]]
AnyFunc = Union[SyncFunc[P, R], AsyncFunc[P, R], SyncGenFunc[P, R], AsyncGenFunc[P, R]]

# Convenience alias for a callable that receives a single context parameter
ContextFunc = Callable[[C], Awaitable[R] | R]

# Utility aliases for callers that don't need explicit type parameters
AnyCallable = Callable[..., Awaitable[Any] | Any]
ContextCallable = Callable[[C], Awaitable[Any] | Any]
