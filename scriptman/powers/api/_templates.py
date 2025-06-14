try:
    from functools import wraps
    from inspect import iscoroutinefunction
    from json import dumps
    from typing import Any, Optional

    from fastapi import status
    from fastapi.responses import JSONResponse
    from loguru import logger
    from pydantic import BaseModel

    from scriptman.core.config import config
    from scriptman.powers.api._exceptions import APIException
    from scriptman.powers.api._models import APIRequest, APIResponse
    from scriptman.powers.api._queue import queue_manager
    from scriptman.powers.generics import Func, P
    from scriptman.powers.serializer import SERIALIZE_FOR_JSON, serialize
    from scriptman.powers.tasks import TaskExecutor
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
    ❌ Creates an error response for a failed API request.

    Args:
        request (APIRequest): The failed API request.
        e (Exception): The exception that occurred.

    Returns:
        dict[str, Any]: Formatted error API response data.
    """
    if not isinstance(e, APIException):
        e = APIException(f"{e.__class__.__name__}: {str(e)}", exception=e)

    logger.error(f"❌ Request {request.request_id} failed with error: {e}")
    logger.debug(f"📤 Request details: \n{dumps(request.model_dump(), indent=4)}")
    logger.debug(f"🔍 Stacktrace: \n{dumps(e.stacktrace, indent=4)}")

    return APIResponse.from_api_exception(request, e).model_dump()


def api_route(
    request: APIRequest,
    func: Func[P, dict[str, Any] | BaseModel],
    timeout: Optional[float] = 30,
    enqueue: bool = False,
) -> Func[P, JSONResponse]:
    """
    🔄 Decorator to wrap synchronous or asynchronous API route functions with
    standardized request/response handling and task execution.

    Args:
        request (APIRequest): The API request object.
        func (Func): The function to wrap.
        timeout (optional, float): The maximum amount of seconds to wait for the task to
            complete.
        enqueue (bool): Enqueues the request task in the background, and return a prompt
            result. If false, will run the task immediately. Default is False.

    Returns:
        Func: Wrapped function with error and success handling.
    """

    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
        try:
            _timeout = config.settings.get("task_timeout", timeout)

            if enqueue:
                await queue_manager.enqueue(
                    func=func,
                    args=args,
                    kwargs=kwargs,
                    request=request,
                    timeout=_timeout,
                )

                # Return immediate response indicating request is queued
                response = APIResponse(
                    request=request,
                    status_code=status.HTTP_202_ACCEPTED,
                    message="Request has been queued for processing.",
                    response={"status": "queued", "request_id": request.request_id},
                ).model_dump(exclude={"stacktrace"})
            else:
                executor = TaskExecutor(
                    thread_pool_size=config.settings.get("thread_pool_size", 50),
                    process_pool_size=config.settings.get("process_pool_size", 4),
                )
                try:
                    if iscoroutinefunction(func):
                        # For async functions, execute directly with await_async
                        result = TaskExecutor.await_async(func(*args, **kwargs))
                    else:
                        # For sync functions, use TaskExecutor's background with timeout
                        task = executor.background(func, *args, **kwargs)
                        result = executor.wait(task, float(_timeout))

                    response = create_successful_response(
                        response=serialize(result, **SERIALIZE_FOR_JSON),
                        request=request,
                    )
                finally:
                    executor.cleanup()

            return JSONResponse(content=response, status_code=response["status_code"])
        except Exception as e:
            response = create_error_response(request=request, e=e)
            return JSONResponse(content=response, status_code=response["status_code"])

    return wrapper
