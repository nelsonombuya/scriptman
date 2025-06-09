try:
    from datetime import datetime
    from decimal import Decimal
    from typing import Any, Callable, ClassVar, Literal, Optional, TypeVar
    from uuid import uuid4

    from loguru import logger
    from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
    from pydantic.main import IncEx
    from typing_extensions import Annotated

    from scriptman.powers.api.exceptions import APIException
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[api]."
    )


class APIRequest(BaseModel):
    """
    🚀 Represents a request object.

    Attributes:
        request_id (str): The unique identifier for the request.
        timestamp (str): The timestamp when the request was created.
        type (str): The type of the request.
        url (str): The URL of the request.
        args (dict[str, Any]): The arguments of the request.
    """

    request_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    type: str = Field(description="The type of the request.")
    url: str = Field(description="The URL of the request.")
    args: dict[str, Any] = Field(default_factory=dict)
    model_config = ConfigDict(extra="ignore")


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
    response_time: Optional[float] = Field(default=None)
    message: str
    status_code: int
    request: APIRequest
    response: Optional[dict[str, Any]] = Field(default=None)
    stacktrace: Optional[list[dict[str, str | int | None]]] = Field(default=None)

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
            raise ValueError(f"Invalid timestamp format: {v}")
        return v

    @field_validator("status_code", mode="before")
    @classmethod
    def not_invalid_status_code(cls, v: int) -> int:
        """Ensure that the status code field is not an invalid status code."""
        if v < 100 or v > 599:
            raise ValueError(f"Invalid status code: {v}")
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
            request=request,
            message=exception.message,
            response=exception.response,
            stacktrace=exception.stacktrace,
            status_code=exception.status_code,
        )


# ⚙ Type Variables
ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)  # Response Model
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
        extra="ignore",  # Ignore extra fields
        frozen=False,  # Allow modification after creation
        str_strip_whitespace=True,  # Strip whitespace from strings
        validate_assignment=True,  # Validate on attribute assignment
        json_encoders={Decimal: lambda v: float(v)},
    )

    _identifier_field: ClassVar[str | None] = None

    def __init__(self, **data: Any) -> None:
        """🚀 Override init to catch and log validation errors."""
        try:
            super().__init__(**data)
        except Exception as e:
            identifier_fields = self.get_identifier_fields()
            identifier = self.get_identifier(identifier_fields[0], data)
            self.log_validation_error(e, identifier, data)
            raise e

    @model_validator(mode="before")
    @classmethod
    def validate_identifier_field(cls, values: dict[str, Any]) -> dict[str, Any]:
        """🆔 Ensures that the inheriting class has defined an entity identifier field."""
        return cls.set_identifier_field(values)

    @classmethod
    def set_identifier_field(cls, values: dict[str, Any]) -> dict[str, Any]:
        if cls._identifier_field is None:
            # Find the field marked with EntityIdentifierField
            identifier_fields = cls.get_identifier_fields()

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
    def get_identifier_fields(cls) -> list[str]:
        """🆔 Returns the identifier fields of the class."""
        from typing import get_type_hints

        return [
            field_name
            for field_name, field_type in get_type_hints(cls, include_extras=True).items()
            if EntityIdentifierField in getattr(field_type, "__metadata__", ())
        ]

    def get_identifier(
        self, field_name: Optional[str] = None, data: Optional[dict[str, Any]] = None
    ) -> str:
        """🆔 Returns the entity's identifier value."""
        if self._identifier_field is None and field_name is None:
            logger.warning(
                f"🔍 Entity {self.__class__.__name__} has no identifier field. "
                "This is likely due to the model not being initialized correctly."
            )
            return "<unknown_identifier>"

        if data:
            return str(
                data.get(field_name or self._identifier_field or "<unknown_identifier>")
            )

        return str(
            getattr(
                self,
                field_name or self._identifier_field or "<unknown_identifier>",
            )
        )

    @model_validator(mode="before")
    @classmethod
    def set_empty_fields_to_none(cls, values: dict[str, Any]) -> dict[str, Any]:
        """🎨 Convert empty string fields to None."""
        if isinstance(values, Exception):
            return values  # Return the exception
        return {
            k: None if isinstance(v, str) and not v.strip() else v
            for k, v in values.items()
        }

    @model_validator(mode="before")
    @classmethod
    def strip_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        """🎨 Strip whitespace from string fields."""
        if isinstance(values, Exception):
            return values  # Return the exception
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
        from email_validator import EmailNotValidError, validate_email

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
        try:
            values = cls.set_identifier_field(values)

            if cls._identifier_field is None:
                raise ValueError(
                    f"Class {cls.__name__} has no identifier field. "
                    "This is likely due to the model not being initialized correctly."
                )

            # HACK: Convert to dictionary if it's a model instance
            if getattr(values, "model_dump", None):
                values = getattr(values, "model_dump")()

            if isinstance(values, Exception):
                return values  # Return the exception

            entity_identifier = values.get(
                cls._identifier_field,
                "unknown",  # Fallback if somehow the identifier isn't set
            )
        except Exception as e:
            logger.error(f"Error getting entity identifier for {cls.__name__}: {str(e)}")
            entity_identifier = "unknown"

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

    def log_validation_error(
        self, error: Exception, identifier: str, data: dict[str, Any]
    ) -> None:
        """📃 Centralized validation error logging."""
        logger.error(
            f"Validation error for {self.__class__.__name__} "
            f"with identifier '{identifier}': {str(error)}\n"
            f"Data: {data}"
        )

    @staticmethod
    def serialize_decimal(value: Optional[Decimal]) -> Optional[float]:
        return float(value) if value is not None else None

    @staticmethod
    def serialize_datetime(value: Optional[datetime], format: str) -> Optional[str]:
        return value.strftime(format) if value is not None else None

    @staticmethod
    def round_to_dp(value: Optional[Decimal], dp: int) -> Optional[Decimal]:
        return value.quantize(Decimal("0." + "0" * dp)) if value is not None else None

    def model_serialize(
        self,
        include: IncEx | None = None,
        exclude: IncEx | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: bool | Literal["none", "warn", "error"] = True,
        fallback: Callable[[Any], Any] | None = None,
        serialize_as_any: bool = False,
    ) -> dict[str, Any]:
        """
        🔄 Serialize the model to a dictionary.

        Args:
            include: Field(s) to include in the JSON output.
            exclude: Field(s) to exclude from the JSON output.
            context: Additional context to pass to the serializer.
            by_alias: Whether to serialize using field aliases.
            exclude_unset: Whether to exclude fields that have not been explicitly set.
            exclude_defaults: Whether to exclude fields that are set to their default
                value.
            exclude_none: Whether to exclude fields that have a value of `None`.
            round_trip: If True, dumped values should be valid as input for non-idempotent
                types such as Json[T].
            warnings: How to handle serialization errors. False/"none" ignores them,
                True/"warn" logs errors, "error" raises a
                [`PydanticSerializationError`][pydantic_core.PydanticSerializationError].
            fallback: A function to call when an unknown value is encountered. If not
                provided, a
                [`PydanticSerializationError`][pydantic_core.PydanticSerializationError]
                error is raised.
            serialize_as_any: Whether to serialize fields with duck-typing serialization
                behavior.

        Returns:
            dict[str, Any]: The serialized model as a dictionary.
        """
        from json import loads
        from typing import cast

        return cast(
            dict[str, Any],
            loads(
                self.model_dump_json(
                    include=include,
                    exclude=exclude,
                    context=context,
                    by_alias=by_alias,
                    warnings=warnings,
                    fallback=fallback,
                    round_trip=round_trip,
                    exclude_none=exclude_none,
                    exclude_unset=exclude_unset,
                    exclude_defaults=exclude_defaults,
                    serialize_as_any=serialize_as_any,
                )
            ),
        )
