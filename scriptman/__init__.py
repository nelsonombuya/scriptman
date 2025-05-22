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

from loguru import logger

# Core imports that are always available
from scriptman.core.config import config

# Powers that are always available
from scriptman.powers.cleanup import CleanUp
from scriptman.powers.concurrency import Task, TaskExecutor, Tasks
from scriptman.powers.generics import AsyncFunc, Func, P, R, SyncFunc, T
from scriptman.powers.retry import retry
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
        api,
    )
    from scriptman.powers.api import exceptions as api_exceptions
except ImportError:
    logger.debug("API Powers Not Available. Install with `pip install scriptman[api]`")

"""
Cache powers
"""
try:
    from scriptman.powers.cache import CacheManager, cache
except ImportError:
    logger.debug(
        "Cache Powers Not Available. Install with `pip install scriptman[cache]`"
    )

"""
Database powers
"""
try:
    from scriptman.powers.database import DatabaseHandler

    try:
        from scriptman.powers.database._sqlalchemy import SQLAlchemyHandler
    except ImportError:
        logger.debug(
            "SQLAlchemy Powers Not Available. "
            "Install with `pip install scriptman[sqlalchemy]`"
        )

    try:
        from scriptman.powers.database._pyodbc import PyODBCHandler
    except ImportError:
        logger.debug(
            "PyODBC Powers Not Available. Install with `pip install scriptman[pyodbc]`"
        )
except ImportError:
    logger.debug(
        "Database Powers Not Available. Install with `pip install scriptman[database]`"
    )


"""
ETL powers
"""
try:
    from scriptman.powers.etl import ETL
except ImportError:
    logger.debug("ETL Powers Not Available. Install with `pip install scriptman[etl]`")


"""
Scheduler powers
"""
try:
    from scriptman.powers.scheduler import Scheduler, scheduler
except ImportError:
    logger.debug(
        "Scheduler Powers Not Available. Install with `pip install scriptman[scheduler]`"
    )


"""
Selenium powers
"""
try:
    from scriptman.powers.selenium import SeleniumInstance
except ImportError:
    logger.debug(
        "Selenium Powers Not Available. Install with `pip install scriptman[selenium]`"
    )


__all__: list[str] = [
    # Core functionality
    # Config
    "config",
    # Logger
    "logger",
    # Concurrency
    "TaskExecutor",
    "Task",
    "Tasks",
    # Generics
    "T",
    "P",
    "R",
    "AsyncFunc",
    "SyncFunc",
    "Func",
    # Retry
    "retry",
    # Cleanup
    "CleanUp",
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
    "scheduler",
    "Scheduler",
    # Selenium
    "SeleniumInstance",
]

# Add version info
__version__ = config.version

# TODO: Script aliases
# TODO: Stop specific script from running
