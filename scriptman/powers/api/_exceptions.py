try:
    import sys
    from traceback import extract_tb
    from typing import Any, Optional

    from fastapi import status
except ImportError:
    raise ImportError(
        "FastAPI is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[api]."
    )


class APIException(Exception):
    """
    🚨 Custom exception class for API errors.

    Args:
        message (str): The concise error message.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.
        status_code (int): The status code for the API Request. Defaults to 500.

    Attributes:
        message (str): The concise error message.
        status_code (int): The HTTP status code.
        exception (Optional[Exception]): The original exception.
        response (Optional[dict[str, Any]]): The response data.
        stacktrace (list[dict]): Structured stacktrace information.
    """

    def __init__(
        self,
        message: str,
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    ):
        super().__init__(message)
        self.message: str = message
        self.status_code: int = status_code
        self.exception: Optional[Exception] = exception
        self.response: Optional[dict[str, Any]] = response
        self.stacktrace: list[dict[str, Any]] = self._generate_stacktrace()

    def _generate_stacktrace(self) -> list[dict[str, str | int | None]]:
        """
        📊 Generates a structured stacktrace.

        Returns:
            list[dict]: A list of dictionaries containing stacktrace information.
        """
        return [
            {
                "frame": index,
                "file": frame.filename,
                "line": frame.lineno,
                "function": frame.name,
                "code": frame.line,
            }
            for index, frame in enumerate(extract_tb(sys.exc_info()[2]), 1)
        ]

    @property
    def to_dict(self) -> dict[str, Any]:
        """
        📊 Converts the exception to a dictionary representation.

        Returns:
            dict[str, Any]: A dictionary containing exception details.
        """
        return {
            "message": self.message,
            "exception": {
                "type": self.exception.__class__.__name__ if self.exception else None,
                "message": str(self.exception) if self.exception else None,
            },
            "response": self.response,
            "status_code": self.status_code,
            "stacktrace": self.stacktrace,
        }
