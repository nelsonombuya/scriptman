try:
    from datetime import datetime
    from typing import Any, Optional, TypeVar
    from uuid import uuid4

    from email_validator import EmailNotValidError
    from loguru import logger
    from pydantic import (
        BaseModel,
        ConfigDict,
        Field,
        field_validator,
        model_validator,
        validate_email,
    )

    from scriptman.powers.api._exceptions import APIException
except ImportError:
    raise ImportError(
        "Pydantic is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[api]."
    )


class APIRequest(BaseModel):
    """
    🚀 Represents a request object.

    Attributes:
        request_id (str): The unique identifier for the request.
        timestamp (str): The timestamp when the request was created.
    """

    request_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    model_config = ConfigDict(extra="forbid")


class APIResponse(BaseModel):
    """
    🚀 Represents a response object.

    Attributes:
        timestamp (str): The timestamp of the response in ISO 8601 format.
            Defaults to the current timestamp.
        response_time (float, optional): The time difference in seconds between
            request and response timestamps.
        message (str): The message of the response.
        status_code (int): The status code of the response.
        request (APIRequest): The details of the request.
        response (dict[str, Any]): The response data.
        stacktrace (list[dict], optional): The structured stacktrace
            information (in case an error occurred). Defaults to None.
    """

    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    response_time: Optional[float] = None
    message: str
    status_code: int
    request: APIRequest
    response: Optional[dict[str, Any]] = None
    stacktrace: Optional[list[dict[str, str | int | None]]] = None

    def __init__(
        self,
        message: str,
        status_code: int,
        request: APIRequest,
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
    def from_api_exception(request: APIRequest, exception: APIException) -> "APIResponse":
        """
        Convert an APIException to a Response object.

        Args:
            exception (APIException): The APIException to convert.

        Returns:
            Response: The converted Response object.
        """
        return APIResponse(
            message=exception.message,
            status_code=exception.status_code,
            request=request,
            response=exception.response,
            stacktrace=exception.stacktrace,
        )


# ⚙ Type Variables
ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)  # Response
EntityModelT = TypeVar("EntityModelT", bound="BaseEntityModel")  # Response Entities


class BaseEntityModel(BaseModel):
    """
    🏗️ BaseEntityModel

    This class extends the Pydantic BaseModel to provide additional functionality for
    handling entity models. It includes validators for converting empty string fields to
    None and stripping whitespace from string fields.

    Methods:
        set_empty_fields_to_none(cls, values: dict) -> dict:
            Convert empty string fields to None.

        strip_fields(cls, values: dict) -> dict:
            Strip whitespace from string fields.
    """

    @model_validator(mode="before")
    @classmethod
    def set_empty_fields_to_none(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        🎨 Convert empty string fields to None.

        Args:
            values (dict): Dictionary of field values.

        Returns:
            dict: Updated dictionary with empty string fields set to None.
        """
        for k, v in values.items():
            if isinstance(v, str) and not v.strip():
                values[k] = None
        return values

    @model_validator(mode="before")
    @classmethod
    def strip_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        🎨 Strip whitespace from string fields.

        Args:
            values (dict): Dictionary of field values.

        Returns:
            dict: Updated dictionary with whitespace stripped from string fields.
        """
        for k, v in values.items():
            if isinstance(v, str):
                values[k] = v.strip()
        return values

    @classmethod
    def validate_and_log_email(
        cls, email: Optional[str], entity_identifier: str, entity_type: str
    ) -> Optional[str]:
        """
        📧 Validate email with centralized logging and handling.

        Args:
            email (Optional[str]): Email to validate
            entity_identifier (str): Unique identifier for the entity (e.g., customer id)
            entity_type (str): Type of entity (e.g., 'Customer', 'Location')

        Returns:
            Optional[str]: Validated email or None if invalid
        """
        try:
            # Custom checks for email validity
            if (
                not email
                or not isinstance(email, str)
                or (isinstance(email, str) and email.strip() == "")
            ):
                raise EmailNotValidError

            # Validate email
            validate_email(email)
            return email

        except EmailNotValidError:
            logger.warning(
                f"{entity_type} {entity_identifier} has invalid email: {email}, "
                "setting it to None."
            )
            return None

    @model_validator(mode="before")
    @classmethod
    def convert_invalid_email_to_none(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        📧 Validate and potentially convert email fields to None.
        """
        email_fields = [
            field for field in values.keys() if str(field).lower().endswith("mail")
        ]

        for email_field in email_fields:
            entity_identifier = values.get("No", values.get("Code", "Unknown"))
            entity_type = cls.__name__.replace("BC", "").replace("Card", "")

            values[email_field] = cls.validate_and_log_email(
                values.get(email_field), entity_identifier, entity_type
            )

        return values
