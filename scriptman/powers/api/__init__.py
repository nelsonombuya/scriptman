from abc import ABC, abstractmethod
from json import dumps
from typing import Any, Generic, Optional

from loguru import logger
from pydantic import ValidationError
from requests import RequestException, Response

from scriptman.powers.api.exceptions import APIException
from scriptman.powers.api.request_handlers import (
    DefaultRequestHandler,
    HTTPMethod,
    RequestHandler,
)
from scriptman.powers.generics import ResponseModelT


class BaseAPIClient(ABC, Generic[ResponseModelT]):
    """🌐 Base class for API client implementations with advanced features:
    - ✅ Response validation via Pydantic models
    - 🛡️ Type-safe response model validation
    - 🎯 Configurable request handling and logging
    """

    def __init__(
        self,
        base_url: str,
        headers: Optional[dict[str, str]] = None,
        request_handler: Optional[RequestHandler] = None,
        default_response_model: Optional[type[ResponseModelT]] = None,
    ):
        """
        🏗️ Initialize API client with base URL, request configuration, and optional
        response model.

        Args:
            base_url (str): Base URL for the API.
            headers (dict, optional): Request headers.
            request_handler (RequestHandler, optional): Custom request handler.
            default_response_model (Type[ResponseModelT], optional): Default response
                model for validation.
        """
        self.base_url = base_url
        self.headers = headers or {}
        self.default_response_model = default_response_model
        self.request_handler: RequestHandler = request_handler or DefaultRequestHandler()

    def request(
        self,
        url: str,
        method: HTTPMethod,
        params: Optional[dict[str, str]] = None,
        body: Optional[dict[str, Any]] = None,
        timeout: Optional[int] = None,
        response_model: Optional[type[ResponseModelT]] = None,
    ) -> ResponseModelT | dict[str, Any]:
        """
        🚀 Send an HTTP request with optional response validation.

        Args:
            url (str): Endpoint URL (relative to base URL).
            method (HTTPMethod): HTTP method.
            params (dict, optional): Query parameters.
            body (dict, optional): Request payload.
            timeout (int, optional): Request timeout in seconds.
            response_model (Type[ResponseModelT], optional): Response validation model.

        Returns:
            ResponseModelT: Validated response data if response_model is provided,
                otherwise raw JSON.

        Raises:
            APIResponseError: When API returns an error response
            ResponseValidationError: When response validation fails
            requests.RequestException: When request fails
        """
        response = self._send_request(self._clean_url(url), method, params, body, timeout)

        if not response_model and not self.default_response_model:
            return response.json()

        return self.validate_response(
            response, response_model or self.default_response_model
        )

    def _clean_url(self, url: str) -> str:
        """
        Clean the URL by removing the base URL and leading and trailing slashes.

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
    ) -> Response:
        """
        Helper method to send the HTTP request and handle HTTP errors.

        Args:
            url (str): Request URL.
            method (HTTPMethod): HTTP method.
            params (dict, optional): Query parameters.
            body (dict, optional): JSON request body.

        Returns:
            Response: HTTP response object.
        """
        try:
            logger.info(f"📤 Sending {method} request to {url}")
            request_data = {"url": url, "method": method, "params": params, "body": body}
            logger.debug(f"📤 Request Details: {dumps(request_data, indent=4)}")

            response = self.request_handler.send(
                url=url,
                method=method,
                headers=self.headers,
                params=params,
                json=body,
                timeout=timeout,
            )
            response.raise_for_status()

            logger.info(f"📤 Received response from {url}")
            logger.debug(f"📤 Response Details: {dumps(response.json(), indent=4)}")
            return response
        except RequestException as e:
            logger.error(f"🔥 Request to {url} failed with error: {e}")
            response_data = e.response.json() if e.response else None
            logger.debug(f"📤 Response Details: {dumps(response_data, indent=4)}")

            raise APIException(
                exception=e,
                message=f"Request to {url} failed with error: {e}",
                response=e.response.json() if e.response else None,
                status_code=e.response.status_code if e.response else 500,
            )

        except Exception as e:
            logger.error(f"🔥 Request to {url} failed with error: {e}")
            raise APIException(
                exception=e,
                status_code=500,
                message=f"Request to {url} failed with error: {e}",
            )

    def validate_response(
        self, response: Response, response_model: Optional[type[ResponseModelT]]
    ) -> ResponseModelT | dict[str, Any]:
        """
        ✅ Validate and parse the JSON response using a Pydantic model.

        Args:
            response (Response): The HTTP response to validate.
            response_model (Type[ResponseModelT]): The Pydantic model for validation.

        Returns:
            ResponseModelT: Parsed and validated response data.

        Raises:
            APIException: If validation fails.
        """
        data = response.json()
        if not response_model:
            self.handle_error_response(response)
            return data

        try:
            return response_model.model_validate(data)
        except ValidationError as e:
            logger.error(f"❌ Response validation failed: {e}")
            raise APIException(f"❌ Response validation failed: {e}", exception=e)

    @abstractmethod
    def handle_error_response(self, response: Response) -> None:
        """
        🔍 Handle errors specific to the API, based on the response.

        Args:
            response (Response): Error response from the API.

        Raises:
            APIException: Custom exception for API errors.
        """
        pass  # pragma: no cover

    @property
    def __generic__(self) -> Optional[type]:
        """🔍 Return the type argument of the generic client, or None."""
        # HACK: Get the type argument of the generic class instance
        return (
            self.__getattribute__("__orig_class__").__getattribute__("__args__")[0]
            if hasattr(self, "__orig_class__")
            else None
        )
