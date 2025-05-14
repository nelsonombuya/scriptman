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
        HTTPMethod,
        ODataV4RequestHandler,
        PostOnlyRequestHandler,
        RequestHandler,
        api,
        exceptions,
    )
except ImportError:
    logger.debug("API powers not available")

"""
Cache powers
"""
try:
    from scriptman.powers.cache import cache
except ImportError:
    logger.debug("Cache powers not available")

"""
Database powers
"""
try:
    from scriptman.powers.database import DatabaseHandler

    try:
        from scriptman.powers.database._sqlalchemy import SQLAlchemyHandler
    except ImportError:
        logger.debug("SQLAlchemy powers not available")

    try:
        from scriptman.powers.database._pyodbc import PyODBCHandler
    except ImportError:
        logger.debug("PyODBC powers not available")
except ImportError:
    logger.debug("Database powers not available")


"""
ETL powers
"""
try:
    from scriptman.powers.etl import ETL
except ImportError:
    logger.debug("ETL powers not available")


"""
Scheduler powers
"""
try:
    from scriptman.powers.scheduler import scheduler
except ImportError:
    logger.debug("Scheduler powers not available")


"""
Selenium powers
"""
try:
    from scriptman.powers.selenium import SeleniumInstance
except ImportError:
    logger.debug("Selenium powers not available")


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
    "exceptions",
    "BaseAPIClient",
    "RequestHandler",
    "BaseEntityModel",
    "EntityIdentifier",
    "DefaultRequestHandler",
    "ODataV4RequestHandler",
    "PostOnlyRequestHandler",
    # Cache
    "cache",
    # Database
    "DatabaseHandler",
    "SQLAlchemyHandler",
    "PyODBCHandler",
    # ETL
    "ETL",
    # Scheduler
    "scheduler",
    # Selenium
    "SeleniumInstance",
]

# Add version info
__version__ = config.version

# TODO: Script aliases
# TODO: Stop specific script from running
