from .config import load_config
from .validation import (QuerySpec, validate_name, parse_iso_date, positive_integer,
                         validate_columns, validate_predicate, validate_spec)
from .exceptions import ExtractionError


class Query:
    """Fluent customer query builder. Multiple where calls are ANDed by execution."""

    def __init__(self, name):
        self._name = validate_name(name)
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
            config = load_config()
            spec = self._validated_spec(config)
            from .executor import run_query
            return run_query(spec, config)
        except ExtractionError as exc:
            print(str(exc), flush=True)
            raise
