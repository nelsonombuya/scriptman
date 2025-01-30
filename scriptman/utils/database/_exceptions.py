from typing import Optional


class DatabaseError(Exception):
    def __init__(self, message: str, original_exception: Optional[Exception] = None):
        """
        Initializes the DatabaseError class.

        Args:
            message (str): The error message.
            original_exception (Exception, optional): The original exception that caused
                this error. Defaults to None.
        """
        super().__init__(message)
        self.original_exception = original_exception
