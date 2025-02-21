try:
    from pathlib import Path
    from uuid import uuid4

    from fastapi import Request
    from loguru import logger
    from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
    from starlette.responses import Response
except ImportError:
    raise ImportError(
        "FastAPI is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[api]."
    )


class FastAPIMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """
        📥 Handles a request and logs its details.

        This middleware sets up a logger to write request logs to a file, and
        logs the request and response details. The request ID is logged as a
        context variable, and is also passed as a header in the response.

        Args:
            request (Request): The incoming request.
            call_next (RequestResponseEndpoint): The next middleware in the chain.

        Returns:
            Response: The response to the request.
        """
        request_id = str(uuid4())

        log_file = (
            Path(__file__).resolve().parent.parent.parent
            / "logs"
            / "requests"
            / f"{request_id}.log"
        )
        log_file.parent.mkdir(parents=True, exist_ok=True)

        request.state.request_id = request_id
        request_handler_id = logger.add(log_file, level="DEBUG", retention="30 days")

        with logger.contextualize(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        ):
            response = await call_next(request)

        response.headers["X-Request-ID"] = request_id
        logger.remove(request_handler_id)
        return response
