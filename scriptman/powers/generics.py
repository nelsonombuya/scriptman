from typing import Any, Callable, Coroutine, TypeVar, Union

from pydantic import BaseModel

# ⚙ Type variables for generic types
T = TypeVar("T")  # For generic argument types
R = TypeVar("R")  # For generic return types (if both T and R are used)

# For synchronous functions
SyncFunc = Callable[..., T]

# For asynchronous functions
AsyncFunc = Callable[..., Coroutine[Any, Any, T]]

# For both sync and async functions
Func = Union[SyncFunc[T], AsyncFunc[T]]

# For generic response types
ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)
