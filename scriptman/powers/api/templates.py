from json import dumps
from typing import Any

from fastapi import status
from loguru import logger

from scriptman.powers.api.exceptions import APIException
from scriptman.powers.api.request import Request
from scriptman.powers.api.response import Response


def create_successful_response(
    request: Request, response: dict[str, Any]
) -> dict[str, Any]:
    """
    ✨ Creates a successful response for a completed request.

    Args:
        request (Request): The completed request.
        response (dict): The response data.

    Returns:
        dict[str, Any]: The response data.
    """
    logger.info(f"✅ Request {request.request_id} completed successfully.")
    logger.debug(f"📤 Request details: \n{dumps(request.model_dump(), indent=4)}")
    return Response(
        request=request,
        response=response.get("response", response),
        message=response.get("message", "Request Successful."),
        status_code=response.get("status_code", status.HTTP_200_OK),
    ).model_dump(exclude={"stacktrace"})


def create_timeout_response(request: Request, task_id: str) -> dict[str, Any]:
    """
    ⏰ Creates a response for a request that timed out.

    Args:
        request (Request): The timed out request.
        task_id (str): The task ID of the background task.

    Returns:
        dict[str, Any]: The response data.
    """
    logger.warning(f"⏳ Request task {task_id} timed out. Continuing in background...")
    logger.debug(f"📤 Request details: \n{dumps(request.model_dump(), indent=4)}")
    return Response(
        request=request,
        status_code=status.HTTP_202_ACCEPTED,
        response={"status": "processing", "task_id": task_id},
        message="Request is taking longer than expected. Please try again later.",
    ).model_dump(exclude={"stacktrace"})


def create_error_response(request: Request, e: Exception) -> dict[str, Any]:
    """
    😓 Creates an error response for a request that experienced an error.

    Args:
        request (Request): The request that experienced an error.
        e (Exception): The exception that was raised.

    Returns:
        dict[str, Any]: The response data.
    """
    if not isinstance(e, APIException):
        e = APIException(f"{e.__class__.__name__}: {str(e)}", exception=e)
    logger.error(f"💥 Request {request.request_id} experienced an error -> {e.message}")
    logger.debug(f"📤 Request details: \n{dumps(request.model_dump(), indent=4)}")
    logger.debug(f"🔍 Stacktrace: \n{dumps(e.stacktrace, indent=4)}")
    return Response.from_api_exception(request, e).model_dump()
