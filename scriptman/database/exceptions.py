"""🚨 Database exceptions."""

from __future__ import annotations


class DatabaseError(Exception):
    """🚨 Base exception for database operations.

    Wraps underlying database errors with context.

    Attributes:
        message: Error description
        original: Original exception that caused this error

    Example:
        >>> try:
        ...     db.execute("INVALID SQL")
        ... except DatabaseError as e:
        ...     print(f"Database error: {e}")
        ...     print(f"Original: {e.original}")
    """

    def __init__(
        self,
        message: str,
        original: Exception | None = None,
    ) -> None:
        """🚀 Initialize DatabaseError.

        Args:
            message: Error description
            original: Original exception
        """
        if original:
            full_message = f"{message}: {type(original).__name__} - {original}"
        else:
            full_message = message

        super().__init__(full_message)
        self.message = message
        self.original = original


__all__ = ["DatabaseError"]
