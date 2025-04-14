from typing import Awaitable, Callable, ParamSpec, TypeVar, Union

from pydantic import BaseModel

# ⚙ Type variables for generic types
T = TypeVar("T")  # For generic argument or return types
P = ParamSpec("P")  # For generic argument types
R = TypeVar("R")  # For generic return types (if both T and R are used)

# For synchronous functions
SyncFunc = Callable[P, R]

# For asynchronous functions
AsyncFunc = Callable[P, Awaitable[R]]

# For both sync and async functions
Func = Union[SyncFunc[P, R], AsyncFunc[P, R]]

# For Pydantic BaseModel
BaseModelT = TypeVar("BaseModelT", bound=BaseModel)
