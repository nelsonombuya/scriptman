"""
This module provides utility functions for the scriptman package.
Available utilities:
- TimeCalculator: A class that calculates the time taken to execute a function or method.
- retry: A decorator function that retries a function or method until it succeeds or a
        maximum number of attempts is reached.

You can import these utilities from scriptman.utils as follows:
```python
from scriptman.utils import retry
```
"""

from scriptman.utils._retry import retry
from scriptman.utils._time_calculator import TimeCalculator

__all__: list[str] = ["TimeCalculator", "retry"]
