try:
    from contextlib import asynccontextmanager
    from inspect import iscoroutinefunction
    from pathlib import Path
    from socket import AF_INET, SOCK_STREAM, socket
    from typing import Any, AsyncGenerator, Callable, Optional

    from fastapi import APIRouter, FastAPI
    from fastapi.responses import JSONResponse
    from loguru import logger
    from uvicorn import run as run_uvicorn_server
    from uvicorn.config import LOGGING_CONFIG

    from scriptman.core.config import config
    from scriptman.powers.api._middleware import FastAPIMiddleware
    from scriptman.powers.api._models import APIRequest
    from scriptman.powers.api._templates import api_route
    from scriptman.powers.generics import Func, P
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[api]."
    )


class APIManager:
    """
    🎯 Singleton manager for FastAPI application instance.
    Provides centralized configuration and startup management.
    """

    _initialized: bool = False
    _app: Optional[FastAPI] = None
    _routers: list[APIRouter] = []
    _instance: Optional["APIManager"] = None
    _startup_handlers: list[Func[..., None]] = []
    _shutdown_handlers: list[Func[..., None]] = []
    _queued_routes: list[
        tuple[str, list[str], Func[..., JSONResponse], dict[str, Any]]
    ] = []

    def __new__(cls, *args: Any, **kwargs: Any) -> "APIManager":
        if cls._instance is None:
            cls._instance = super(APIManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        if not self._initialized:
            self._port = config.secrets.get("api.port", self._find_available_port())
            self._host = config.secrets.get("api.host", "0.0.0.0")
            self._configured = False
            self._initialized = True

    @staticmethod
    def _find_available_port(start_port: int = 8000, max_attempts: int = 100) -> int:
        """Find an available port starting from start_port."""
        for port in range(start_port, start_port + max_attempts):
            with socket(AF_INET, SOCK_STREAM) as s:
                try:
                    s.bind(("", port))
                    return port
                except OSError:
                    continue
        raise RuntimeError(
            f"Could not find an available port after {max_attempts} attempts"
        )

    def route(
        self, path: str, methods: list[str] = ["GET"], **kwargs: Any
    ) -> Callable[[Func[P, dict[str, Any]]], Func[P, JSONResponse]]:
        """
        Decorator for adding routes directly to the API.
        Routes are queued and will be added during application startup.

        Args:
            path: URL path for the route
            methods: HTTP methods to support
            **kwargs: Additional FastAPI route options
        """

        def decorator(func: Func[P, dict[str, Any]]) -> Func[P, JSONResponse]:
            request = APIRequest(url=path, type=methods[0], args=kwargs)
            template_func: Func[P, JSONResponse] = api_route(request, func)
            self._queued_routes.append((path, methods, template_func, kwargs))
            logger.info(f"Queued route {path} with methods {methods}")
            return template_func

        return decorator

    def get(
        self, path: str, **kwargs: Any
    ) -> Callable[[Func[P, dict[str, Any]]], Func[P, JSONResponse]]:
        """
        Decorator for adding GET routes to the API.

        Args:
            path: URL path for the route
            **kwargs: Additional FastAPI route options
        """
        return self.route(path, methods=["GET"], **kwargs)

    def post(
        self, path: str, **kwargs: Any
    ) -> Callable[[Func[P, dict[str, Any]]], Func[P, JSONResponse]]:
        """
        Decorator for adding POST routes to the API.

        Args:
            path: URL path for the route
            **kwargs: Additional FastAPI route options
        """
        return self.route(path, methods=["POST"], **kwargs)

    def put(
        self, path: str, **kwargs: Any
    ) -> Callable[[Func[P, dict[str, Any]]], Func[P, JSONResponse]]:
        """
        Decorator for adding PUT routes to the API.

        Args:
            path: URL path for the route
            **kwargs: Additional FastAPI route options
        """
        return self.route(path, methods=["PUT"], **kwargs)

    def delete(
        self, path: str, **kwargs: Any
    ) -> Callable[[Func[P, dict[str, Any]]], Func[P, JSONResponse]]:
        """
        Decorator for adding DELETE routes to the API.

        Args:
            path: URL path for the route
            **kwargs: Additional FastAPI route options
        """
        return self.route(path, methods=["DELETE"], **kwargs)

    def patch(
        self, path: str, **kwargs: Any
    ) -> Callable[[Func[P, dict[str, Any]]], Func[P, JSONResponse]]:
        """
        Decorator for adding PATCH routes to the API.

        Args:
            path: URL path for the route
            **kwargs: Additional FastAPI route options
        """
        return self.route(path, methods=["PATCH"], **kwargs)

    def configure(
        self,
        title: str = "ScriptMan API",
        description: str = "API powered by ScriptMan",
        version: str = config.version,
        host: str = "0.0.0.0",
        port: Optional[int] = None,
        **kwargs: Any,
    ) -> "APIManager":
        """
        Configure the FastAPI application with custom settings.

        Args:
            title: API title
            description: API description
            version: API version
            host: Host to bind to
            port: Port to use (if None, will find an available port)
            **kwargs: Additional FastAPI configuration options
        """
        self._host = host
        if port is not None:
            self._port = port

        self._app = FastAPI(
            title=title,
            version=version,
            lifespan=self.lifespan,
            description=description,
            **kwargs,
        )

        self._app.add_middleware(FastAPIMiddleware)
        self._configured = True
        return self

    @property
    def app(self) -> FastAPI:
        """Get the FastAPI application instance, creating a default one if needed."""
        if self._app is None:
            logger.info("Creating default FastAPI application")
            self.configure()
        assert self._app, "FastAPI application is not configured"
        return self._app

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        """
        Manage the application lifespan, executing startup and shutdown handlers.

        Args:
            app (FastAPI): The FastAPI application instance.

        Yields:
            None: Used to indicate the lifespan management context.
        """
        # Add queued routes during startup
        for path, methods, template_func, kwargs in self._queued_routes:
            self.app.add_api_route(
                endpoint=template_func,
                methods=methods,
                path=path,
                **kwargs,
            )
            logger.info(f"Added queued route {path} with methods {methods}")

        # Clear the queue after adding routes
        self._queued_routes.clear()

        for handler in self._startup_handlers:  # Startup
            await handler() if iscoroutinefunction(handler) else handler()

        yield

        for handler in self._shutdown_handlers:  # Shutdown
            await handler() if iscoroutinefunction(handler) else handler()

    def add_router(self, router: APIRouter, prefix: str = "", **kwargs: Any) -> None:
        """Add a router to the application."""
        self.app.include_router(router, prefix=prefix, **kwargs)

    def add_startup_handler(self, handler: Func[..., None]) -> None:
        """Add a startup handler to be executed when the application starts."""
        self._startup_handlers.append(handler)

    def add_shutdown_handler(self, handler: Func[..., None]) -> None:
        """Add a shutdown handler to be executed when the application shuts down."""
        self._shutdown_handlers.append(handler)

    def run(
        self, host: Optional[str] = None, port: Optional[int] = None, **kwargs: Any
    ) -> None:
        """
        Run the FastAPI application using uvicorn.

        Args:
            host: Override the configured host
            port: Override the configured port
            **kwargs: Additional uvicorn configuration options
        """
        final_host = self._host = host or self._host
        final_port = self._port = port or self._port

        logger.info("🔧 Configuring logging")
        fmt = f"\033[92m%(asctime)s\033[0m | \033[1m{'UVICORN':<8}\033[0m | %(message)s"
        LOGGING_CONFIG["formatters"]["default"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
        LOGGING_CONFIG["formatters"]["default"]["fmt"] = fmt

        logger.info(f"📡 Starting API server at http://{final_host}:{final_port}")
        run_uvicorn_server(self.app, host=final_host, port=final_port, **kwargs)

    def initialize_api_module(self, file_path: str = "api.py") -> None:
        """🚀 Initialize the API module."""
        file = Path(config.cwd / file_path)
        file.parent.mkdir(parents=True, exist_ok=True)

        if not file.exists():
            file.write_text(
                "# Basic FastAPI server setup\n"
                "# Import the API manager from scriptman\n"
                "from scriptman.powers.api import api\n"
                "\n"
                "# Start the API server with default configuration\n"
                "# You can customize the host/port by calling api.configure() first\n"
                "api.run()"
            )

        logger.success(
            "\n"
            f"✨ API module initialized successfully at {file_path}\n"
            "\n"
            "Quick Start:\n"
            "  from scriptman.powers.api import api\n"
            "  api.run()\n"
            "\n"
            "Available Commands:\n"
            "  scriptman api --start    Start the API server\n"
            "\n"
            "API Configuration:\n"
            "  api.configure()              Set API options\n"
            "  api.add_router()             Add route handlers\n"
            "  api.route()                  Add route handlers\n"
            "  api.add_startup_handler()    Add startup hooks\n"
            "  api.add_shutdown_handler()   Add shutdown hooks\n"
            "  api.run()                    Start the API server\n"
        )


# Singleton Instance
api: APIManager = APIManager()
__all__: list[str] = ["api"]
