"""
This module provides utility functions for the scriptman package.
Available utilities:
- generics: A collection of generic utility functions for various purposes.
- retry: A decorator function that retries a function or method until it succeeds or a
        maximum number of attempts is reached.
- TaskExecutor: A class that executes tasks concurrently using asyncio.
- TimeCalculator: A class that calculates the time taken to execute a function or method.

You can import these utilities from scriptman.utils as follows:
```python
from scriptman.utils import retry
```
"""

from scriptman.utils import generics
from scriptman.utils.cleanup import CleanUp
from scriptman.utils.concurrency import BatchResult, TaskExecutor, TaskResult
from scriptman.utils.retry import Retry
from scriptman.utils.time_calculator import TimeCalculator

__all__: list[str] = [
    "generics",
    "Retry",
    "CleanUp",
    "TaskExecutor",
    "TaskResult",
    "BatchResult",
    "TimeCalculator",
]
