try:
    from abc import ABC
    from json import dumps
    from typing import Any, Generic, Optional

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
        logger.debug(f"📡 Sending {method.value} request to {url}")
        logger.debug(f"📡 Request Details: {dumps(kwargs, indent=4)}")
        response = raw_request(method.value, url, **kwargs)
        response.raise_for_status()
        logger.info(f"✅ {method.value} request for {url} completed successfully.")
        logger.debug(f"📤 Response Details: {dumps(response.json(), indent=4)}")
        return response

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
        response_model = self._get_response_model(response_model)
        data: dict[str, Any] = response.json()

        return (
            data
            if not response_model
            else self.validate_response(response, response_model)
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
            logger.info(f"📤 Sending {method.value} request to {url}")
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

            raise exceptions.APIException(
                exception=e,
                message=f"Request to {url} failed with error: {e}",
                response=e.response.json() if e.response else None,
                status_code=e.response.status_code if e.response else 500,
            )

        except Exception as e:
            logger.error(f"🔥 Request to {url} failed with error: {e}")
            raise exceptions.APIException(
                exception=e,
                status_code=500,
                message=f"Request to {url} failed with error: {e}",
            )

    def _get_response_model(
        self, response_model: Optional[type[ResponseModelT]] = None
    ) -> Optional[type[ResponseModelT]]:
        """
        🍱 Get the response model for the current request.

        - If the response_model is specified, use it.
        - If the default_response_model is specified and the generic type is provided,
            use the default_response_model for the generic type.
        - If the default_response_model is specified and the generic type is not provided,
            use the default_response_model.
        - Otherwise, return None.

        Args:
            response_model (Type[ResponseModelT], optional): The response model to use.

        Returns:
            Type[ResponseModelT]: The response model to use.
        """
        return (
            response_model
            if response_model
            else (
                # HACK: Getting the generic for the response model
                self.default_response_model[self.generic]  # type:ignore
                if self.generic
                and self.default_response_model
                and self.default_response_model != self.generic
                else self.default_response_model if self.default_response_model else None
            )
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
            exceptions.APIException: If validation fails.
        """
        data: dict[str, Any] = response.json()
        if not response_model:
            return data

        try:
            return response_model.model_validate(data)
        except ValidationError as e:
            logger.error(f"❌ Response validation failed: {e}")
            raise exceptions.ValidationError(
                f"❌ Response validation failed: {e}",
                exception=e,
                errors={
                    str(i): {"loc": e["loc"], "msg": e["msg"], "input": e["input"]}
                    for i, e in enumerate(e.errors())
                },
            )

    @staticmethod
    def get_generic(object: Any) -> Optional[type]:
        """🔍 Return the type argument of the generic client, or None."""
        return (  # HACK: Get the type argument of the generic class instance
            object.__getattribute__("__orig_class__").__getattribute__("__args__")[0]
            if hasattr(object, "__orig_class__")
            else None
        )

    @property
    def generic(self) -> Optional[type]:
        """
        👤 The type argument of the generic client, or None.

        This can be used to access the type argument of the generic client
        from within the client class itself.

        Returns:
            Optional[type]: The type argument of the generic client, or None.
        """
        return self.get_generic(self)


__all__: list[str] = [
    "api",
    "api_route",
    "HTTPMethod",
    "exceptions",
    "EntityModelT",
    "BaseAPIClient",
    "RequestHandler",
    "ResponseModelT",
    "async_api_route",
    "BaseEntityModel",
    "EntityIdentifier",
    "DefaultRequestHandler",
    "ODataV4RequestHandler",
    "PostOnlyRequestHandler",
]
