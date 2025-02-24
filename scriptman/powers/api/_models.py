try:
    from datetime import datetime
    from decimal import Decimal
    from typing import Any, ClassVar, Optional, TypeVar, get_type_hints
    from uuid import uuid4

    from email_validator import EmailNotValidError, validate_email
    from loguru import logger
    from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
    from typing_extensions import Annotated

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


class EntityIdentifierField:
    # Marker class for entity identifier fields
    pass


EntityIdentifier = Annotated[str, EntityIdentifierField]


class BaseEntityModel(BaseModel):
    """
    🏗️ BaseEntityModel

    An enhanced base class for entity models with built-in validation, logging, and error
    handling. Requires inheriting classes to specify an entity identifier field for better
    error tracking.

    Features:
    - Automatic empty string to None conversion
    - Whitespace stripping for string fields
    - Email validation with logging
    - Required entity identifier field
    - Configurable model settings

    Example:
        ```python
        class Customer(BaseEntityModel):
            customer_id: EntityIdentifier = Field(description="Unique customer id")
            name: str
            email: Optional[str] = None

        # The customer_id field will be used in error messages and logging
        ```
    """

    model_config = ConfigDict(
        extra="forbid",  # Forbid extra fields
        frozen=False,  # Allow modification after creation
        str_strip_whitespace=True,  # Strip whitespace from strings
        validate_assignment=True,  # Validate on attribute assignment
    )

    _identifier_field: ClassVar[str | None] = None

    @model_validator(mode="before")
    @classmethod
    def validate_identifier_field(cls, values: dict[str, Any]) -> dict[str, Any]:
        """🆔 Ensures that the inheriting class has defined an entity identifier field."""
        if cls._identifier_field is None:
            # Find the field marked with EntityIdentifierField
            hints = get_type_hints(cls, include_extras=True)
            identifier_fields = [
                field_name
                for field_name, field_type in hints.items()
                if EntityIdentifierField in getattr(field_type, "__metadata__", ())
            ]

            if not identifier_fields:
                raise ValueError(
                    f"Class {cls.__name__} must define exactly one field annotated with "
                    "EntityIdentifier type for entity identification"
                )
            if len(identifier_fields) > 1:
                raise ValueError(
                    f"Class {cls.__name__} has multiple EntityIdentifier fields: "
                    f"{identifier_fields}. Only one is allowed."
                )

            cls._identifier_field = identifier_fields[0]

        return values

    @classmethod
    def get_identifier(cls) -> str:
        """🆔 Returns the entity's identifier value."""
        if cls._identifier_field is None:
            raise RuntimeError("Entity identifier field not initialized")
        return str(getattr(cls, cls._identifier_field))

    @model_validator(mode="before")
    @classmethod
    def set_empty_fields_to_none(cls, values: dict[str, Any]) -> dict[str, Any]:
        """🎨 Convert empty string fields to None."""
        return {
            k: None if isinstance(v, str) and not v.strip() else v
            for k, v in values.items()
        }

    @model_validator(mode="before")
    @classmethod
    def strip_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        """🎨 Strip whitespace from string fields."""
        return {k: v.strip() if isinstance(v, str) else v for k, v in values.items()}

    @classmethod
    def validate_and_log_email(
        cls, email: Optional[str], entity_identifier: str, field_name: str
    ) -> Optional[str]:
        """
        📧 Validate email with enhanced error logging.

        Args:
            email: Email to validate
            entity_identifier: Identifier of the entity
            field_name: Name of the email field being validated

        Returns:
            Validated email or None if invalid
        """
        if not email or not isinstance(email, str) or not email.strip():
            return None

        try:
            return validate_email(email).normalized

        except (EmailNotValidError, ValueError) as e:
            logger.warning(
                f"{cls.__name__} with identifier '{entity_identifier}' has invalid "
                f"email in field '{field_name}': {email!r}. Error: {str(e)}"
            )
            return None

    @model_validator(mode="before")
    @classmethod
    def validate_email_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        """📧 Validate all email fields in the model."""
        entity_identifier = values.get(
            cls._identifier_field or "identifier",
            "unknown",  # Fallback if somehow the identifier isn't set
        )

        # Find all email fields (those ending with 'email' or 'mail')
        email_fields = [
            field
            for field in values.keys()
            if str(field).lower().endswith(("email", "mail"))
        ]

        for field in email_fields:
            values[field] = cls.validate_and_log_email(
                values.get(field), entity_identifier, field
            )

        return values

    @classmethod
    def log_validation_error(cls, error: Exception) -> None:
        """📃 Centralized validation error logging."""
        logger.error(
            f"Validation error for {cls.__class__.__name__} "
            f"with identifier '{cls.get_identifier()}': {str(error)}"
        )

    @staticmethod
    def serialize_decimal(value: Optional[Decimal]) -> Optional[float]:
        return float(value) if value is not None else None

    @staticmethod
    def serialize_datetime(value: Optional[datetime], format: str) -> Optional[str]:
        return value.strftime(format) if value is not None else None

    @staticmethod
    def round_to_dp(value: Optional[Decimal], dp: int = 2) -> Optional[Decimal]:
        return round(value, dp) if value is not None else None
