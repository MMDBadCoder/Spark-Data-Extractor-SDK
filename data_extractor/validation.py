import re
from dataclasses import dataclass
from datetime import date
from .exceptions import ExtractionError


@dataclass(frozen=True)
class QuerySpec:
    name: str
    start_date: date
    end_date: date
    predicates: tuple
    columns: tuple | None
    row_limit: int
    file_count: int


def validate_name(name):
    if not isinstance(name, str) or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}', name) is None:
        raise ExtractionError('INVALID_NAME')
    return name


def parse_iso_date(value):
    if not isinstance(value, str) or re.fullmatch(r'\d{4}-\d{2}-\d{2}', value) is None:
        raise ExtractionError('INVALID_DATE')
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ExtractionError('INVALID_DATE') from exc


def positive_integer(value, code, maximum=None):
    if type(value) is not int or value < 1 or (maximum is not None and value > maximum):
        raise ExtractionError(code)
    return value


def validate_columns(columns):
    if any(not isinstance(c, str) or not c.strip() or '\x00' in c for c in columns) or len(set(columns)) != len(columns):
        raise ExtractionError('INVALID_COLUMN')
    return tuple(columns) if columns else None


def validate_predicate(predicate):
    classes = []
    try:
        from pyspark.sql import Column
        classes.append(Column)
    except ImportError:
        pass
    try:
        from pyspark.sql.connect.column import Column as ConnectColumn
        classes.append(ConnectColumn)
    except ImportError:
        pass
    if not classes or not isinstance(predicate, tuple(classes)):
        raise ExtractionError('INVALID_FILTER')
    return predicate


def validate_spec(spec, config):
    validate_name(spec.name)
    if type(spec.start_date) is not date or type(spec.end_date) is not date:
        raise ExtractionError('INVALID_DATE')
    days = (spec.end_date - spec.start_date).days + 1
    if not 1 <= days <= min(config.max_days, 7):
        raise ExtractionError('INVALID_DATE_RANGE')
    positive_integer(spec.row_limit, 'INVALID_LIMIT', min(config.max_limit, 1_000_000_000))
    positive_integer(spec.file_count, 'INVALID_FILE_COUNT', config.max_output_files)
    if (spec.row_limit + spec.file_count - 1) // spec.file_count > config.max_planned_rows_per_file:
        from .messages import message
        required = (spec.row_limit + config.max_planned_rows_per_file - 1) // config.max_planned_rows_per_file
        raise ExtractionError('TOO_MANY_ROWS_PER_FILE', message('TOO_MANY_ROWS_PER_FILE', required=required))
    validate_columns(spec.columns or ())
    for predicate in spec.predicates:
        validate_predicate(predicate)
    return spec
