"""🦸‍♂️ Scriptman.

Scriptman is a batteries-included automation toolkit for web automation, ETL,
and workflow scheduling. Importing from the top-level package gives you:

* **Task execution** – `TaskManager` coordinates background jobs, multithreaded
  execution, and cooperative services.
* **Service infrastructure** – simple decorators for registering long-lived
  loops that integrate with TaskManager lifecycle management.
* **Scheduling utilities** – interval, daily, and one-off triggers coupled with
  a decorator-friendly scheduler façade.
* **Quality-of-life helpers** – retry logic, cleanup primitives, serializers,
  Selenium automation, and other conveniences used across Scriptman powers.

The ``scriptman/docs`` directory (for example, the ETL quick-start and
architecture guides) expands on these exports with deeper usage notes. Whether
you are wiring cron-like jobs, exposing background services, or orchestrating
data pipelines, importing from :mod:`scriptman` gives you the curated surface
area needed to build and run automations quickly.
"""

from loguru import logger

# Core imports that are always available
from scriptman.core.config import config

# Powers that are always available
from scriptman.powers.cleanup import CleanUp
from scriptman.powers.generics import AsyncFunc, Func, P, R, SyncFunc, T
from scriptman.powers.retry import retry
from scriptman.powers.scheduler import IntervalTrigger
from scriptman.powers.scheduler import Job as SchedulerJob
from scriptman.powers.scheduler import (
    OneTimeTrigger,
    SchedulerService,
    SchedulerTrigger,
    TaskScheduler,
    TimeOfDayTrigger,
    scheduler,
)
from scriptman.powers.serializer import (
    SERIALIZE_FOR_CACHE,
    SERIALIZE_FOR_JSON,
    SERIALIZE_FOR_PICKLE,
    serialize,
)
from scriptman.powers.service import (
    ServiceCallable,
    ServiceContext,
    ServiceDefinition,
    ServiceManager,
    ServiceRegistry,
    service_manager,
)
from scriptman.powers.tasks import Task, TaskManager, Tasks
from scriptman.powers.time_calculator import TimeCalculator

# Optional powers that depend on extra packages
"""
API powers
"""
try:
    from scriptman.powers.api import (
        BaseAPIClient,
        BaseEntityModel,
        DefaultRequestHandler,
        EntityIdentifier,
        EntityModelT,
        HTTPMethod,
        ODataV4RequestHandler,
        PostOnlyRequestHandler,
        RequestHandler,
        ResponseModelT,
    )
    from scriptman.powers.api import _exceptions as api_exceptions
    from scriptman.powers.api import api
except ImportError as e:
    logger.warning(
        "Scriptman API Powers are unavailable. "
        "You may install them with `pip install scriptman[api]`"
    )
    logger.debug(f"API Powers Import Error: {e}")

"""
Cache powers
"""
try:
    from scriptman.powers.cache import CacheManager, cache
except ImportError as e:
    logger.warning(
        "Scriptman Cache Powers are unavailable. "
        "You may install them with `pip install scriptman[cache]`"
    )
    logger.debug(f"Cache Powers Import Error: {e}")

"""
Database powers
"""
try:
    from scriptman.powers.database import DatabaseHandler
except ImportError as e:
    logger.warning(
        "Scriptman Database Powers are unavailable. "
        "You may install them with `pip install scriptman[database]`"
    )
    logger.debug(f"Database Powers Import Error: {e}")

try:
    from scriptman.powers.database._sqlalchemy import SQLAlchemyHandler
except ImportError as e:
    logger.warning(
        "Scriptman SQLAlchemy Powers are unavailable. "
        "You may install them with `pip install scriptman[sqlalchemy]`"
    )
    logger.debug(f"SQLAlchemy Powers Import Error: {e}")

try:
    from scriptman.powers.database._pyodbc import PyODBCHandler
except ImportError as e:
    logger.warning(
        "Scriptman PyODBC Powers are unavailable. "
        "You may install them with `pip install scriptman[pyodbc]`"
    )
    logger.debug(f"PyODBC Powers Import Error: {e}")


"""
ETL powers
"""
try:
    from scriptman.powers.etl import ETL
except ImportError as e:
    logger.warning(
        "Scriptman ETL Powers are unavailable. "
        "You may install them with `pip install scriptman[etl]`"
    )
    logger.debug(f"ETL Powers Import Error: {e}")


"""
Selenium powers
"""
try:
    from scriptman.powers.selenium import SeleniumInstance
except ImportError as e:
    logger.warning(
        "Scriptman Selenium Powers are unavailable. "
        "You may install them with `pip install scriptman[selenium]`"
    )
    logger.debug(f"Selenium Powers Import Error: {e}")


__all__: list[str] = [
    # Core functionality
    # Cleanup
    "CleanUp",
    # Concurrency
    "TaskManager",
    "Task",
    "Tasks",
    # Config
    "config",
    # Generics
    "T",
    "P",
    "R",
    "AsyncFunc",
    "SyncFunc",
    "Func",
    # Logger
    "logger",
    # Retry
    "retry",
    # Serializer
    "SERIALIZE_FOR_CACHE",
    "SERIALIZE_FOR_JSON",
    "SERIALIZE_FOR_PICKLE",
    "serialize",
    # Time calculator
    "TimeCalculator",
    # Optional functionality
    # API
    "api",
    "HTTPMethod",
    "EntityModelT",
    "BaseAPIClient",
    "ResponseModelT",
    "api_exceptions",
    "RequestHandler",
    "BaseEntityModel",
    "EntityIdentifier",
    "DefaultRequestHandler",
    "ODataV4RequestHandler",
    "PostOnlyRequestHandler",
    # Cache
    "cache",
    "CacheManager",
    # Database
    "DatabaseHandler",
    "SQLAlchemyHandler",
    "PyODBCHandler",
    # ETL
    "ETL",
    # Scheduler
    "TaskScheduler",
    "SchedulerJob",
    "IntervalTrigger",
    "TimeOfDayTrigger",
    "OneTimeTrigger",
    "SchedulerTrigger",
    "SchedulerService",
    "scheduler",
    # Services
    "ServiceDefinition",
    "ServiceManager",
    "ServiceContext",
    "ServiceRegistry",
    "ServiceCallable",
    "service_manager",
    # Selenium
    "SeleniumInstance",
]

# Scriptman insignia
"""
######################################################################################
##                                                                                  ##
## #######  #######  #######  ######   ####### ########  ##   ##  #######  ###  ##  ##
##                        ##    ##          ##    ##     ### ###       ##  #### ##  ##
## #######  ##       #######    ##     #######    ##     #######  #######  ## ####  ##
##      ##  ##       ##  ##     ##     ##         ##     ## # ##  ##   ##  ##  ###  ##
## #######  #######  ##   ##  ######   ##         ##     ##   ##  ##   ##  ##   ##  ##
##                                                                                  ##
######################################################################################
"""

# Add version info
__version__ = config.version

# TODO: Script aliases
# TODO: Stop specific script from running
