from asyncio import Queue, QueueFull, Task, create_task
from dataclasses import dataclass
from typing import Any, Optional

from loguru import logger

from scriptman.core.config import config
from scriptman.powers.api._models import APIRequest
from scriptman.powers.api.exceptions import APIException
from scriptman.powers.concurrency import TaskExecutor
from scriptman.powers.generics import Func, P, R


@dataclass
class QueuedRequest:
    """📦 Container for a queued API request"""

    request: APIRequest
    func: Func[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    executor: TaskExecutor
    timeout: float


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
            self._processing_task: Optional[Task[None]] = None
            queue_size = config.settings.get("task_queue_size", 100)
            self._queue: Queue[QueuedRequest] = Queue(maxsize=queue_size)
            self._started = False
            self.__class__._initialized = True

    async def start(self) -> None:
        """Start the queue processing task"""
        if not self._started and (
            self._processing_task is None or self._processing_task.done()
        ):
            self._processing_task = create_task(self._process_queue())
            self._started = True
            logger.info("🚦 Started API queue manager")

    async def _process_queue(self) -> None:
        """Process queued requests"""
        while True:
            try:
                queued_request = await self._queue.get()
                try:
                    if queued_request.func.__name__ == "<lambda>":
                        # For async functions wrapped in lambda
                        await queued_request.func(
                            *queued_request.args,
                            **queued_request.kwargs,
                        )
                    else:
                        # For regular functions
                        queued_request.executor.wait(
                            queued_request.executor.background(
                                queued_request.func,
                                *queued_request.args,
                                **queued_request.kwargs,
                            ),
                            timeout=queued_request.timeout,
                        )
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
                    queued_request.executor.cleanup()
                    self._queue.task_done()
            except Exception as e:
                logger.error(f"❌ Error in queue processing: {e}")

    async def enqueue(
        self,
        request: APIRequest,
        func: Func[P, R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        timeout: float = 30,
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

        executor = TaskExecutor(
            thread_pool_size=config.settings.get("thread_pool_size", 50),
            process_pool_size=config.settings.get("process_pool_size", 4),
        )
        queued_request = QueuedRequest(
            request=request,
            func=func,
            args=args,
            kwargs=kwargs,
            executor=executor,
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
        """Shutdown the queue manager"""
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except Exception:
                pass
            logger.info("🛑 Shut down API queue manager")
        self._started = False


# Singleton instance
queue_manager = APIQueueManager()
