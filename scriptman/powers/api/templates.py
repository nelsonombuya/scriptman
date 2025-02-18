try:
    from functools import wraps
    from json import dumps
    from typing import Any

    from fastapi import status
    from loguru import logger

    from scriptman.powers.api.exceptions import APIException
    from scriptman.powers.api.models import APIRequest, APIResponse
    from scriptman.powers.generics import AsyncFunc, SyncFunc
except ImportError:
    raise ImportError(
        "FastAPI is not installed. "
        "Kindly install the dependencies using 'scriptman[api]'."
    )


def create_successful_response(
    request: APIRequest, response: dict[str, Any]
) -> dict[str, Any]:
    """
    ✅ Creates a successful response for a completed API request.

    Args:
        request (APIRequest): The completed API request.
        response (dict): The response data.

    Returns:
        dict[str, Any]: Formatted successful API response data.
    """
    logger.info(f"✅ Request {request.request_id} completed successfully.")
    logger.debug(f"📤 Request details: \n{dumps(request.model_dump(), indent=4)}")
    return APIResponse(
        request=request,
        response=response.get("response", response),
        message=response.get("message", "Request Successful."),
        status_code=response.get("status_code", status.HTTP_200_OK),
    ).model_dump(exclude={"stacktrace"})


def create_timeout_response(request: APIRequest, task_id: str) -> dict[str, Any]:
    """
    ⏳ Creates a timeout response when a request exceeds the expected duration.

    Args:
        request (APIRequest): The API request that timed out.
        task_id (str): Identifier for the background task.

    Returns:
        dict[str, Any]: Formatted timeout API response data.
    """
    logger.warning(f"⏳ Request task {task_id} timed out. Continuing in background...")
    logger.debug(f"📤 Request details: \n{dumps(request.model_dump(), indent=4)}")
    return APIResponse(
        request=request,
        status_code=status.HTTP_202_ACCEPTED,
        response={"status": "processing", "task_id": task_id},
        message="Request is taking longer than expected. Please try again later.",
    ).model_dump(exclude={"stacktrace"})


def create_error_response(request: APIRequest, e: Exception) -> dict[str, Any]:
    """
    💥 Creates an error response for a failed API request.

    Args:
        request (APIRequest): The API request that encountered an error.
        e (Exception): The raised exception.

    Returns:
        dict[str, Any]: Formatted error API response data.
    """
    if not isinstance(e, APIException):
        e = APIException(f"{e.__class__.__name__}: {str(e)}", exception=e)

    logger.error(f"💥 Request {request.request_id} failed -> {e.message}")
    logger.debug(f"📤 Request details: \n{dumps(request.model_dump(), indent=4)}")
    logger.debug(f"🔍 Stacktrace: \n{dumps(e.stacktrace, indent=4)}")

    return APIResponse.from_api_exception(request, e).model_dump()


def api_route(func: SyncFunc[dict[str, Any]]) -> SyncFunc[dict[str, Any]]:
    """
    🔄 Decorator to wrap synchronous API route functions with standardized
    request/response handling.

    Args:
        func (SyncFunc): The synchronous function to wrap.

    Returns:
        SyncFunc: Wrapped function with error and success handling.
    """

    @wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
        request = APIRequest()
        try:
            response = func(*args, **kwargs)
            return create_successful_response(request=request, response=response)
        except Exception as e:
            return create_error_response(request=request, e=e)

    return sync_wrapper


async def async_api_route(func: AsyncFunc[dict[str, Any]]) -> AsyncFunc[dict[str, Any]]:
    """
    🔄⚡ Decorator to wrap asynchronous API route functions with standardized
    request/response handling.

    Args:
        func (AsyncFunc): The asynchronous function to wrap.

    Returns:
        AsyncFunc: Wrapped async function with error and success handling.
    """

    @wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
        request = APIRequest()
        try:
            response = await func(*args, **kwargs)
            return create_successful_response(request=request, response=response)
        except Exception as e:
            return create_error_response(request=request, e=e)

    return async_wrapper
