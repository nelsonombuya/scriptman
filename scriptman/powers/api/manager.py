try:
    from asyncio import iscoroutinefunction
    from contextlib import asynccontextmanager
    from pathlib import Path
    from socket import AF_INET, SOCK_STREAM, socket
    from typing import Any, AsyncGenerator, Optional

    from fastapi import APIRouter, FastAPI
    from loguru import logger
    from uvicorn import run as run_uvicorn_server

    from scriptman.core.config import config
    from scriptman.powers.api.middleware import FastAPIMiddleware
    from scriptman.powers.generics import Func
except ImportError:
    raise ImportError(
        "FastAPI is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[api]."
    )


class FastAPIManager:
    """
    🎯 Singleton manager for FastAPI application instance.
    Provides centralized configuration and startup management.
    """

    _initialized: bool = False
    _app: Optional[FastAPI] = None
    _startup_handlers: list[Func[None]] = []
    _shutdown_handlers: list[Func[None]] = []
    _instance: Optional["FastAPIManager"] = None

    def __new__(cls) -> "FastAPIManager":
        if cls._instance is None:
            cls._instance = super(FastAPIManager, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not self._initialized:
            self._port = self._find_available_port()
            self._host = "0.0.0.0"
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

    def configure(
        self,
        title: str = "ScriptMan API",
        description: str = "API powered by ScriptMan",
        version: str = config.version,
        host: str = "0.0.0.0",
        port: Optional[int] = None,
        **kwargs: Any,
    ) -> "FastAPIManager":
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
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None]:
        """
        Manage the application lifespan, executing startup and shutdown handlers.

        Args:
            app (FastAPI): The FastAPI application instance.

        Yields:
            None: Used to indicate the lifespan management context.
        """
        for handler in self._startup_handlers:  # Startup
            await handler() if iscoroutinefunction(handler) else handler()

        yield

        for handler in self._shutdown_handlers:  # Shutdown
            await handler() if iscoroutinefunction(handler) else handler()

    def add_router(self, router: APIRouter, prefix: str = "", **kwargs: Any) -> None:
        """Add a router to the application."""
        self.app.include_router(router, prefix=prefix, **kwargs)

    def add_startup_handler(self, handler: Func[None]) -> None:
        """Add a startup handler to be executed when the application starts."""
        self._startup_handlers.append(handler)

    def add_shutdown_handler(self, handler: Func[None]) -> None:
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
        final_host = host or self._host
        final_port = port or self._port

        logger.info(f"📡 Starting API server at http://{final_host}:{final_port}")
        run_uvicorn_server(self.app, host=final_host, port=final_port, **kwargs)

    def initialize_api_module(self) -> None:
        """🚀 Initialize the API module."""
        file = Path(config.cwd / "api" / "__init__.py")
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch(exist_ok=True)
        logger.success(
            "Successfully initialized api module. "
            "Kindly import scriptman.powers.api.api_manager to proceed."
        )


# Singleton Instance
api: FastAPIManager = FastAPIManager()
__all__: list[str] = ["api"]
