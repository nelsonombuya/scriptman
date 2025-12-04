"""🔐 Internal utilities — not part of the public API.

These modules are for internal Scriptman use only. External users
should import from the main scriptman package instead.

Contents:
    - log: Safe logging that handles circular imports
"""

from scriptman._internal import log

__all__ = ["log"]


