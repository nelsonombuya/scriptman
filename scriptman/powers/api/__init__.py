try:
    from abc import ABC
    from json import dumps
    from typing import Any, Optional, overload

    from loguru import logger
    from pydantic import ValidationError
    from requests import RequestException, Response
    from requests import request as raw_request

    from scriptman.powers.api import exceptions
    from scriptman.powers.api._handlers import (
        DefaultRequestHandler,
        HTTPMethod,
        ODataV4RequestHandler,
        PostOnlyRequestHandler,
        RequestHandler,
    )
    from scriptman.powers.api._manager import api
    from scriptman.powers.api._models import (
        BaseEntityModel,
        EntityIdentifier,
        EntityModelT,
        ResponseModelT,
    )
    from scriptman.powers.api._templates import api_route, async_api_route

except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[api]."
    )


class BaseAPIClient(ABC):
    """🌐 Base class for API client implementations with advanced features:
    - ✅ Response validation via Pydantic models
    - 🛡️ Type-safe response model validation
    - 🎯 Configurable request handling and logging
    """

    def __init__(
        self,
        base_url: str,
        headers: Optional[dict[str, str]] = None,
        request_handler: RequestHandler = DefaultRequestHandler(),
    ):
        """
        🏗️ Initialize API client with base URL, request configuration, and optional
        response model.

        Args:
            base_url (str): Base URL for the API.
            headers (dict, optional): Request headers.
            request_handler (RequestHandler): Custom request handler.
                Defaults to DefaultRequestHandler() which uses the `requests` library.
        """
        self.log = logger
        self.base_url = base_url
        self.headers = headers or {}
        self.request_handler: RequestHandler = request_handler

    def raw_request(self, method: HTTPMethod, url: str, **kwargs: Any) -> Response:
        """
        📡 Send a raw HTTP request.

        Args:
            method (HTTPMethod): HTTP method.
            url (str): Request URL.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            Response: HTTP response object.
        """
        self.log.debug(f"📡 Sending {method.value} request to {url}")
        self.log.debug(f"📡 Request Details: {dumps(kwargs, indent=4)}")
        response = raw_request(method.value, url, **kwargs)
        response.raise_for_status()
        self.log.info(f"✅ {method.value} request for {url} completed successfully.")
        self.log.debug(f"📤 Response Details: {dumps(response.json(), indent=4)}")
        return response

    @overload
    def request(
        self,
        url: str,
        method: HTTPMethod,
        response_model: type[ResponseModelT],
        params: Optional[dict[str, str]] = None,
        body: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
        rate_limit_waiting_time: int = 60,
    ) -> ResponseModelT:
        """🚀 Send an HTTP request with strongly-typed response validation."""
        ...

    @overload
    def request(
        self,
        url: str,
        method: HTTPMethod,
        response_model: None = None,
        params: Optional[dict[str, str]] = None,
        body: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
        rate_limit_waiting_time: int = 60,
    ) -> dict[str, Any]:
        """🚀 Send an HTTP request without response validation."""
        ...

    def request(
        self,
        url: str,
        method: HTTPMethod,
        response_model: Optional[type[ResponseModelT]] = None,
        params: Optional[dict[str, str]] = None,
        body: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
        rate_limit_waiting_time: int = 60,
    ) -> ResponseModelT | dict[str, Any]:
        """
        🚀 Send an HTTP request with optional response validation.

        Args:
            url (str): Endpoint URL (relative to base URL).
            method (HTTPMethod): HTTP method.
            response_model (Type[BaseModel], optional): Response validation model.
            params (dict, optional): Query parameters.
            body (dict, optional): Request payload.
            timeout (int, optional): Request timeout in seconds.
            rate_limit_waiting_time (int, optional): Waiting time for rate limit in
                seconds.

        Returns:
            ResponseModelT: Validated response data as the specific model type if
                response_model is provided, otherwise raw JSON dictionary.

        Raises:
            APIResponseError: When API returns an error response
            ResponseValidationError: When response validation fails
            requests.RequestException: When request fails
        """
        response = self._send_request(
            rate_limit_waiting_time=rate_limit_waiting_time,
            url=self._clean_url(url),
            timeout=timeout,
            method=method,
            params=params,
            body=body,
        )
        data: dict[str, Any] = response.json()

        if not response_model:
            return data

        return self.validate_response(response, response_model)

    def _clean_url(self, url: str) -> str:
        """
        🧹 Clean the URL by removing the base URL and leading and trailing slashes.

        Args:
            url (str): URL to clean.

        Returns:
            str: Cleaned URL.
        """
        url = url.replace(self.base_url, "").strip("/").replace("//", "")
        return f"{self.base_url.rstrip('/')}/{url}"

    def _send_request(
        self,
        url: str,
        method: HTTPMethod,
        params: Optional[dict[str, str]],
        body: Optional[dict[str, Any]],
        timeout: Optional[int] = None,
        rate_limit_waiting_time: int = 60,
    ) -> Response:
        """
        📩 Helper method to send the HTTP request and handle HTTP errors.

        Args:
            url (str): Request URL.
            method (HTTPMethod): HTTP method.
            params (dict, optional): Query parameters.
            body (dict, optional): JSON request body.

        Returns:
            Response: HTTP response object.
        """
        try:
            self.log.info(f"📤 Sending {method.value} request to {url}")
            request_data = {"url": url, "method": method, "params": params, "body": body}
            self.log.debug(f"📤 Request Details: {dumps(request_data, indent=4)}")

            response = self.request_handler.send(
                url=url,
                method=method,
                headers=self.headers,
                params=params,
                json=body,
                timeout=timeout,
            )
            response.raise_for_status()

            self.log.info(f"📤 Received response from {url}")
            self.log.debug(f"📤 Response Details: {dumps(response.json(), indent=4)}")
            return response
        except RequestException as e:
            self.log.debug(f"Exception type: {type(e).__name__}")
            self.log.debug(f"Exception structure: {vars(e)}")

            if e.response is not None and e.response.status_code == 429:
                from time import sleep

                self.log.info(
                    "🔴 Rate limit exceeded. "
                    f"Waiting for {rate_limit_waiting_time} seconds."
                )
                sleep(rate_limit_waiting_time)
                return self._send_request(
                    url, method, params, body, timeout, rate_limit_waiting_time
                )

            self.log.error(f"🔥 Request to {url} failed with error: {e}")
            response_data = e.response.json() if e.response else None
            self.log.debug(f"📤 Response Details: {dumps(response_data, indent=4)}")

            raise exceptions.APIException(
                exception=e,
                message=f"Request to {url} failed with error: {e}",
                response=e.response.json() if e.response else None,
                status_code=e.response.status_code if e.response else 500,
            )

        except Exception as e:
            self.log.error(f"🔥 Request to {url} failed with error: {e}")
            raise exceptions.APIException(
                exception=e,
                status_code=500,
                message=f"Request to {url} failed with error: {e}",
            )

    def validate_response(
        self, response: Response, response_model: type[ResponseModelT]
    ) -> ResponseModelT:
        """
        ✅ Validate and parse the JSON response using a Pydantic model.

        Args:
            response (Response): The HTTP response to validate.
            response_model (Type[ResponseModelT]): The Pydantic model for validation.

        Returns:
            ResponseModelT: Parsed and validated response data.

        Raises:
            exceptions.APIException: If validation fails.
        """

        try:
            return response_model.model_validate(response.json())
        except ValidationError as e:
            self.log.error(f"❌ Response validation failed: {e}")
            raise exceptions.ValidationError(
                f"❌ Response validation failed: {e}",
                exception=e,
                errors={
                    str(i): {"loc": e["loc"], "msg": e["msg"], "input": e["input"]}
                    for i, e in enumerate(e.errors())
                },
            )


__all__: list[str] = [
    "api",
    "api_route",
    "HTTPMethod",
    "exceptions",
    "EntityModelT",
    "BaseAPIClient",
    "RequestHandler",
    "async_api_route",
    "BaseEntityModel",
    "EntityIdentifier",
    "DefaultRequestHandler",
    "ODataV4RequestHandler",
    "PostOnlyRequestHandler",
]
