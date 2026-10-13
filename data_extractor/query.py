from .config import load_config
from .validation import (QuerySpec, validate_name, parse_iso_date, positive_integer,
                         validate_columns, validate_predicate, validate_spec)
from .exceptions import ExtractionError
from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool


def _runtime_config():
    """Load and apply the configured Connect dispatch before users import col."""
    import os

    config = load_config()
    configured = config.connect_url
    existing = os.environ.get('SPARK_REMOTE')
    if existing and existing != configured:
        raise ExtractionError('CONFIG_ERROR', 'نشانی Spark فعال با تنظیم مدیر یکسان نیست؛ هستهٔ دفترچه را بازنشانی و تنظیمات را بررسی کنید.')
    os.environ['SPARK_REMOTE'] = configured
    return config


class Query:
    """Fluent customer query builder. Multiple where calls are ANDed by execution."""

    def __init__(self, name):
        try:
            self._name = validate_name(name)
            _runtime_config()
        except ExtractionError as exc:
            print(str(exc), flush=True)
            raise
        self._start = self._end = None
        self._predicates = []
        self._columns = None
        self._limit = self._files = None

    def between(self, start, end):
        start, end = parse_iso_date(start), parse_iso_date(end)
        if not 1 <= (end - start).days + 1 <= 7:
            raise ExtractionError('INVALID_DATE_RANGE')
        self._start, self._end = start, end
        return self

    def where(self, predicate):
        self._predicates.append(validate_predicate(predicate))
        return self

    def select(self, *columns):
        self._columns = validate_columns(columns)
        return self

    def limit(self, n):
        self._limit = positive_integer(n, 'INVALID_LIMIT', 1_000_000_000)
        return self

    def output_files(self, n):
        self._files = positive_integer(n, 'INVALID_FILE_COUNT')
        return self

    def _validated_spec(self, config):
        return validate_spec(QuerySpec(self._name, self._start, self._end,
                             tuple(self._predicates), self._columns,
                             self._limit, self._files), config)

    def run(self):
        try:
            config = _runtime_config()
            spec = self._validated_spec(config)
            from .executor import run_query
            return run_query(spec, config)
        except ExtractionError as exc:
            print(str(exc), flush=True)
            raise

    def show_columns(self):
        """Print and return the source schema for this query's date range."""
        try:
            config = _runtime_config()
            validate_name(self._name)
            if self._start is None or self._end is None:
                raise ExtractionError('DATES_REQUIRED')
            days = (self._end - self._start).days + 1
            if not 1 <= days <= min(config.max_days, 7):
                raise ExtractionError('INVALID_DATE_RANGE')
            from .executor import discover_columns
            return discover_columns(self._name, self._start, self._end, config)
        except ExtractionError as exc:
            print(str(exc), flush=True)
            raise
