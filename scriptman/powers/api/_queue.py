try:
    from asyncio import (
        CancelledError,
        Queue,
        QueueFull,
        Semaphore,
        Task,
        create_task,
        get_event_loop,
        sleep,
        wait_for,
    )
    from dataclasses import dataclass
    from inspect import iscoroutinefunction
    from typing import Any, Optional

    from loguru import logger

    from scriptman.core.config import config
    from scriptman.powers.api._exceptions import APIException
    from scriptman.powers.api._models import APIRequest
    from scriptman.powers.generics import Func, P, R
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies using 'scriptman[api]'."
    )


@dataclass
class QueuedRequest:
    """📦 Container for a queued API request"""

    request: APIRequest
    func: Func[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    timeout: Optional[float] = None


class APIQueueManager:
    """🚦 Manages API request queue and processing"""

    _instance: Optional["APIQueueManager"] = None
    _initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> "APIQueueManager":
        if cls._instance is None:
            cls._instance = super(APIQueueManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        """Initialize the queue manager"""
        if not self._initialized:
            queue_size = config.settings.get("task_queue_size", 100)
            self._queue: Queue[QueuedRequest] = Queue(maxsize=queue_size)
            max_concurrent_requests = config.settings.get("max_concurrent_requests", 25)
            self._semaphore = Semaphore(max_concurrent_requests)
            self._processing_task: Optional[Task[None]] = None
            self._active_tasks: set[Task[None]] = set()
            self._started = False
            self._is_shutting_down = False
            self.__class__._initialized = True

    async def start(self) -> None:
        """Start the queue processing task"""
        if not self._started:
            self._started = True
            self._is_shutting_down = False
            self._processing_task = create_task(self._process_queue())
            logger.info("🚦 Started API queue manager")

    async def _process_queue(self) -> None:
        """Process queued requests concurrently"""
        while not self._is_shutting_down:
            try:
                queued_request = await self._queue.get()
                task = create_task(self._process_request(queued_request))
                self._active_tasks.add(task)
                task.add_done_callback(lambda t: self._active_tasks.discard(t))
            except CancelledError:
                if not self._is_shutting_down:
                    logger.warning("Queue processing was cancelled unexpectedly")
                break
            except Exception as e:
                logger.error(f"❌ Error in queue processing: {e}")
                if not self._is_shutting_down:
                    await sleep(1)  # Prevent tight loop on errors

    async def _process_request(self, queued_request: QueuedRequest) -> None:
        """Process a single request with concurrency control"""
        async with self._semaphore:  # Limit concurrent requests
            try:
                # Execute with timeout if specified
                if queued_request.timeout:
                    await wait_for(
                        self._execute_function(queued_request),
                        timeout=queued_request.timeout,
                    )
                else:
                    await self._execute_function(queued_request)

                logger.info(
                    "✅ Processed queued request with id "
                    + queued_request.request.request_id
                )
            except Exception as e:
                logger.error(
                    f"❌ Failed to process queued request "
                    f"{queued_request.request.request_id}: {e}"
                )
            finally:
                self._queue.task_done()

    async def _execute_function(self, queued_request: QueuedRequest) -> Any:
        """Execute the function directly - async or sync"""
        func = queued_request.func
        args = queued_request.args
        kwargs = queued_request.kwargs

        if iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            loop = get_event_loop()
            from functools import partial

            func_with_args = partial(func, *args, **kwargs)
            return await loop.run_in_executor(None, func_with_args)

    async def enqueue(
        self,
        request: APIRequest,
        func: Func[P, R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        timeout: Optional[float] = None,
    ) -> None:
        """
        Add a request to the processing queue

        Args:
            request: The API request to queue
            func: The function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            timeout: Maximum time to wait for execution
        """
        if not self._started:
            await self.start()

        queued_request = QueuedRequest(
            request=request,
            func=func,
            args=args,
            kwargs=kwargs,
            timeout=timeout,
        )
        try:
            await self._queue.put(queued_request)
            logger.info(f"📥 Queued request {request.request_id}")
        except QueueFull:
            raise APIException(
                "Request queue is full. Please try again later.",
                status_code=503,
            )

    async def shutdown(self) -> None:
        """Shutdown the queue manager gracefully"""
        if not self._started:
            return

        logger.info("🛑 Shutting down API queue manager...")
        self._is_shutting_down = True

        # Wait for queue to be empty
        if not self._queue.empty():
            logger.info("⏳ Waiting for queued tasks to complete...")
            await self._queue.join()

        # Wait for all active request processing tasks to complete
        if self._active_tasks:
            logger.info(f"⏳ Waiting for {len(self._active_tasks)} tasks to complete...")
            while self._active_tasks:
                await sleep(0.1)

        # Cancel the processing task
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except CancelledError:
                pass

        self._started = False
        logger.info("✅ API queue manager shutdown complete")


# Singleton instance
queue_manager = APIQueueManager()
