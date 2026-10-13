"""Spark Connect extraction SDK. Importing it has no Spark side effects."""
from .query import Query, ColumnInfo
from .result import Result
from .exceptions import ExtractionError

__all__ = ['Query', 'Result', 'ColumnInfo', 'ExtractionError']
