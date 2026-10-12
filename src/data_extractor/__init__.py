"""Spark Connect extraction SDK."""
import os

# This SDK targets Connect exclusively. Enable public SQL function dispatch before
# customers build native Column expressions; no session is created at import.
os.environ.setdefault('SPARK_CONNECT_MODE_ENABLED', '1')

from .query import Query
from .result import Result
from .exceptions import ExtractionError

__all__ = ['Query', 'Result', 'ExtractionError']
