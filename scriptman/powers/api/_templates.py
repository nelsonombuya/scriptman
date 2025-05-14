try:
    from functools import wraps
    from inspect import iscoroutinefunction
    from json import dumps, loads
    from typing import Any, cast

    from fastapi import status
    from fastapi.responses import JSONResponse
    from loguru import logger
    from pydantic import BaseModel

    from scriptman.powers.api._models import APIRequest, APIResponse
    from scriptman.powers.api.exceptions import APIException
    from scriptman.powers.concurrency import TaskExecutor
    from scriptman.powers.generics import Func, P
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
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


def api_route(
    request: APIRequest, func: Func[P, dict[str, Any]]
) -> Func[P, JSONResponse]:
    """
    🔄 Decorator to wrap synchronous or asynchronous API route functions with
    standardized request/response handling.

    Args:
        func (Func): The synchronous function to wrap.

    Returns:
        Func: Wrapped function with error and success handling.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
        try:
            if iscoroutinefunction(func):
                result = TaskExecutor.await_async(func(*args, **kwargs))
                result = cast(dict[str, Any], result)
            else:
                result = cast(dict[str, Any], func(*args, **kwargs))

            result = pickle_values(result)
            response = create_successful_response(request=request, response=result)
            return JSONResponse(content=response, status_code=response["status_code"])
        except Exception as e:
            response = create_error_response(request=request, e=e)
            return JSONResponse(content=response, status_code=response["status_code"])

    return wrapper


def pickle_values(data: dict[str, Any]) -> dict[str, Any]:
    """📦 Pickles values in a dictionary."""
    result = {}
    for key, value in data.items():
        try:
            logger.debug(f"Pickling value {value} for key '{key}'")
            dumps(value)  # Try if the value is picklable
        except (TypeError, OverflowError):
            if isinstance(value, BaseModel):
                result[key] = loads(value.model_dump_json())
            else:
                result[key] = str(value)
    return result
