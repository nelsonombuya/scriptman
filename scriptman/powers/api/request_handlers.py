from enum import Enum
from typing import Any, Optional, Protocol

from requests import Response, request


class HTTPMethod(str, Enum):
    """🚦 HTTP Methods enum for type safety"""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class RequestHandler(Protocol):
    """📡 Protocol for request handling"""

    def send(
        self,
        url: str,
        method: HTTPMethod,
        headers: dict[str, Any],
        params: Optional[dict[str, str]] = None,
        json: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> Response:
        """🚀 Sends a request"""
        ...  # pragma: no cover


class DefaultRequestHandler:
    """🌐 Default implementation of request handling"""

    def send(
        self,
        url: str,
        method: HTTPMethod,
        headers: dict[str, Any],
        params: Optional[dict[str, str]] = None,
        json: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> Response:
        return request(
            url=url,
            method=method.value,
            headers=headers,
            params=params,
            json=json,
            timeout=timeout,
        )


class PostOnlyRequestHandler(DefaultRequestHandler):
    """📡 Request handler that forces all requests to use POST"""

    def send(
        self,
        url: str,
        method: HTTPMethod,
        headers: dict[str, Any],
        params: Optional[dict[str, str]] = None,
        json: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> Response:
        return request(
            url=url,
            method="POST",
            headers=headers,
            params=params,
            json=json,
            timeout=timeout,
        )


class ODataV4RequestHandler(DefaultRequestHandler):
    """🌐 Request handler that formats parameters according to OData v4 standards"""

    def send(
        self,
        url: str,
        method: HTTPMethod,
        headers: dict[str, Any],
        params: Optional[dict[str, str]] = None,
        json: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> Response:
        return request(
            url=url,
            method=method.value,
            headers=headers,
            params=(
                {f"${key}": value for key, value in params.items()} if params else None
            ),
            json=json,
            timeout=timeout,
        )
