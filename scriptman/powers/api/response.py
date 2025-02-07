from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from scriptman.powers.api.exceptions import APIException
from scriptman.powers.api.request import Request


class Response(BaseModel):
    """
    🚀 Represents a response object.

    Attributes:
        timestamp (str): The timestamp of the response in ISO 8601 format.
            Defaults to the current timestamp.
        response_time (float, optional): The time difference in seconds between
            request and response timestamps.
        message (str): The message of the response.
        status_code (int): The status code of the response.
        request (Request): The details of the request.
        response (dict[str, Any]): The response data.
        stacktrace (list[dict], optional): The structured stacktrace
            information (in case an error occurred). Defaults to None.
    """

    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    response_time: Optional[float] = None
    message: str
    status_code: int
    request: Request
    response: Optional[dict[str, Any]] = None
    stacktrace: Optional[list[dict[str, str | int | None]]] = None

    def __init__(
        self,
        message: str,
        status_code: int,
        request: Request,
        response: Optional[dict[str, Any]] = None,
        stacktrace: Optional[list[dict[str, str | int | None]]] = None,
    ) -> None:
        """
        Initialize Response model with response time calculation.

        Args:
            message (str): The message of the response.
            status_code (int): The status code of the response.
            request (Request): The details of the request.
            response (dict[str, Any], optional): The response data.
            stacktrace (list[dict], optional): The structured stacktrace
                information (in case an error occurred). Defaults to None.

        Automatically calculates response time after model initialization.
        Uses the request timestamp and current response timestamp to compute
        the difference in seconds.
        """
        super().__init__(
            **{
                "message": message,
                "request": request,
                "response": response,
                "stacktrace": stacktrace,
                "status_code": status_code,
            }
        )
        request_timestamp = datetime.fromisoformat(self.request.timestamp)
        response_timestamp = datetime.fromisoformat(self.timestamp)
        response_time = response_timestamp - request_timestamp
        self.response_time = response_time.total_seconds()

    @field_validator("message", mode="before")
    @classmethod
    def not_empty_string(cls, v: str) -> str:
        """Ensure that the message field is not empty."""
        if not v or v.isspace():
            raise ValueError("Message cannot be empty")
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def not_invalid_timestamp(cls, v: str) -> str:
        """Ensure that the timestamp field is not an invalid timestamp."""
        try:
            datetime.fromisoformat(v)
        except ValueError:
            raise ValueError("Invalid timestamp format")
        return v

    @field_validator("status_code", mode="before")
    @classmethod
    def not_invalid_status_code(cls, v: int) -> int:
        """Ensure that the status code field is not an invalid status code."""
        if v < 100 or v > 599:
            raise ValueError("Invalid status code")
        return v

    @staticmethod
    def from_api_exception(request: Request, exception: APIException) -> "Response":
        """
        Convert an APIException to a Response object.

        Args:
            exception (APIException): The APIException to convert.

        Returns:
            Response: The converted Response object.
        """
        return Response(
            message=exception.message,
            status_code=exception.status_code,
            request=request,
            response=exception.response,
            stacktrace=exception.stacktrace,
        )
