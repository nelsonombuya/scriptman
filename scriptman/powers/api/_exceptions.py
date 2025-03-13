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


class NotFoundError(APIException):
    """
    🔍 Exception raised when a requested resource is not found.

    Args:
        message (str): The concise error message.
        resource_type (str): The type of resource that wasn't found (e.g., "User").
        resource_id (str): The identifier of the resource that wasn't found.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/users/{user_id}")
        def get_user(user_id: str):
            user = find_user_by_id(user_id)  # Your user lookup function

            if not user:
                # This will return a 404 status code in the HTTP response
                raise NotFoundError(
                    message="",  # Empty message will be auto-populated
                    resource_type="User",
                    resource_id=user_id
                )

            # Return the user data if found
            return {"user": user}
        ```
    """

    def __init__(
        self,
        message: str,
        resource_type: str,
        resource_id: str,
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
    ):
        if not message:
            message = f"{resource_type} with ID '{resource_id}' not found."

        # Create a response dictionary with useful details
        if response is None:
            response = {
                "error": "not_found",
                "resource_type": resource_type,
                "resource_id": resource_id,
            }

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_404_NOT_FOUND,
        )


class BadRequestError(APIException):
    """
    🚫 Exception raised when a request is invalid or malformed.

    Args:
        message (str): The concise error message.
        details (dict[str, Any]): Detailed information about the validation errors.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/users", methods=["POST"])
        def create_user(user_data: dict):
            if not user_data.get("email"):
                raise BadRequestError(
                    message="Email is required",
                    details={"email": "This field is required"}
                )
            # Process valid request...
        ```
    """

    def __init__(
        self,
        message: str,
        details: Optional[dict[str, Any]] = None,
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
    ):
        # Create a response dictionary with useful details
        if response is None:
            response = {"error": "bad_request", "details": details or {}}

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_400_BAD_REQUEST,
        )


class UnauthorizedError(APIException):
    """
    🔒 Exception raised when authentication is required but not provided or invalid.

    Args:
        message (str): The concise error message.
        auth_scheme (str): The authentication scheme that failed (e.g., "Bearer", "Basic")
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/protected-resource")
        def get_protected_resource(request):
            if not request.headers.get("Authorization"):
                raise UnauthorizedError(
                    message="Authentication required",
                    auth_scheme="Bearer"
                )
            # Process authenticated request...
        ```
    """

    def __init__(
        self,
        message: str,
        auth_scheme: str = "Bearer",
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
    ):
        if response is None:
            response = {"error": "unauthorized", "auth_scheme": auth_scheme}

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_401_UNAUTHORIZED,
        )


class ForbiddenError(APIException):
    """
    🚷 Exception raised when a user is authenticated but lacks permission.

    Args:
        message (str): The concise error message.
        required_permission (Optional[str]): The permission that would be required.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/admin/settings")
        def admin_settings(user):
            if not user.is_admin:
                raise ForbiddenError(
                    message="Only administrators can access this resource",
                    required_permission="admin"
                )
            # Process admin request...
        ```
    """

    def __init__(
        self,
        message: str,
        required_permission: Optional[str] = None,
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
    ):
        if response is None:
            response = {"error": "forbidden", "required_permission": required_permission}

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_403_FORBIDDEN,
        )


class ConflictError(APIException):
    """
    ⚠️ Exception raised when there's a conflict with the current state of a resource.

    Args:
        message (str): The concise error message.
        resource_type (str): The type of resource with the conflict.
        resource_id (str): The identifier of the conflicting resource.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/users", methods=["POST"])
        def create_user(user_data: dict):
            if user_exists(user_data["email"]):
                raise ConflictError(
                    message="User with this email already exists",
                    resource_type="User",
                    resource_id=user_data["email"]
                )
            # Create user...
        ```
    """

    def __init__(
        self,
        message: str,
        resource_type: str,
        resource_id: str,
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
    ):
        if response is None:
            response = {
                "error": "conflict",
                "resource_type": resource_type,
                "resource_id": resource_id,
            }

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_409_CONFLICT,
        )


class TooManyRequestsError(APIException):
    """
    🛑 Exception raised when a client exceeds the rate limit.

    Args:
        message (str): The concise error message.
        retry_after (int): The number of seconds to wait before retrying.
        limit (Optional[int]): The rate limit that was exceeded.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/api/resource")
        def rate_limited_endpoint(client_id):
            if exceeded_rate_limit(client_id):
                raise TooManyRequestsError(
                    message="Rate limit exceeded",
                    retry_after=60,
                    limit=100
                )
            # Process request...
        ```
    """

    def __init__(
        self,
        message: str,
        retry_after: int,
        limit: Optional[int] = None,
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
    ):
        if response is None:
            response = {
                "error": "too_many_requests",
                "retry_after": retry_after,
                "limit": limit,
            }

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )


class ValidationError(APIException):
    """
    ❌ Exception raised when input validation fails.

    Args:
        message (str): The concise error message.
        errors (dict[str, Any]): Validation errors by field.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/users", methods=["POST"])
        def create_user(user_data: dict):
            errors = validate_user_data(user_data)
            if errors:
                raise ValidationError(
                    message="Invalid user data",
                    errors=errors
                )
            # Create valid user...
        ```
    """

    def __init__(
        self,
        message: str,
        errors: dict[str, Any],
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
    ):
        if response is None:
            response = {
                "error": "validation_error",
                "errors": errors,
            }

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )


class ServiceUnavailableError(APIException):
    """
    🔌 Exception raised when the service is temporarily unavailable.

    Args:
        message (str): The concise error message.
        retry_after (Optional[int]): The number of seconds to wait before retrying.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/api/backup")
        def backup_data():
            if system_is_in_maintenance():
                raise ServiceUnavailableError(
                    message="System is in maintenance mode",
                    retry_after=3600  # Try again in an hour
                )
            # Process backup request...
        ```
    """

    def __init__(
        self,
        message: str,
        retry_after: Optional[int] = None,
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
    ):
        headers = {}
        if retry_after:
            headers["Retry-After"] = str(retry_after)

        if response is None:
            response = {
                "error": "service_unavailable",
                "retry_after": retry_after,
            }

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )


class PaymentRequiredError(APIException):
    """
    💰 Exception raised when payment is required to access a resource.

    Args:
        message (str): The concise error message.
        plan_info (Optional[dict]): Information about required subscription plans.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/api/premium-feature")
        def premium_feature(user):
            if not user.has_premium_subscription:
                raise PaymentRequiredError(
                    message="This feature requires a premium subscription",
                    plan_info={"name": "Premium", "price": "$9.99/month"}
                )
            # Provide premium feature...
        ```
    """

    def __init__(
        self,
        message: str,
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
        plan_info: Optional[dict[str, Any]] = None,
    ):
        if response is None:
            response = {
                "error": "payment_required",
                "plan_info": plan_info,
            }

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
        )


class MethodNotAllowedError(APIException):
    """
    🚫 Exception raised when an HTTP method is not allowed for a resource.

    Args:
        message (str): The concise error message.
        method (str): The HTTP method that was used.
        allowed_methods (list[str]): List of allowed HTTP methods.
        exception (Optional[Exception]): The original exception that occurred.
        response (Optional[dict[str, Any]]): The response data associated with the error.

    Example:
        ```python
        @api.route("/api/users/{user_id}")
        def user_endpoint(user_id: str, request):
            if request.method == "DELETE" and not is_admin(request.user):
                raise MethodNotAllowedError(
                    message="DELETE method not allowed for regular users",
                    method="DELETE",
                    allowed_methods=["GET", "PUT", "PATCH"]
                )
            # Process allowed request...
        ```
    """

    def __init__(
        self,
        message: str,
        method: str,
        allowed_methods: list[str],
        exception: Optional[Exception] = None,
        response: Optional[dict[str, Any]] = None,
    ):
        if response is None:
            response = {
                "error": "method_not_allowed",
                "method": method,
                "allowed_methods": allowed_methods,
            }

        super().__init__(
            message=message,
            exception=exception,
            response=response,
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )
