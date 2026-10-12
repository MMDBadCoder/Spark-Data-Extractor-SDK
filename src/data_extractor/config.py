import os
import math
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from .exceptions import ExtractionError


@dataclass(frozen=True)
class Config:
    connect_url: str | None = None
    session_prefix: str = 'extract'
    connect_timeout_seconds: float = 60
    execution_timeout_seconds: float = 3600
    hdfs_uri: str | None = None
    input_base: str = '/warehouse/source'
    output_base: str = '/warehouse'
    max_days: int = 7
    max_limit: int = 1_000_000_000
    max_output_files: int = 200
    max_planned_rows_per_file: int = 2_000_000
    max_records_per_file: int = 2_000_000
    timezone: str = 'Asia/Tehran'
    target_compressed_mb: float | None = None
    webhdfs_base_url: str | None = None
    webhdfs_timeout_seconds: float = 60
    webhdfs_user: str | None = None
    webhdfs_token: str | None = field(default=None, repr=False)
    log_file: str = str(Path.home() / '.cache/data_extractor/extraction.log')
    lock_file: str = str(Path.home() / '.cache/data_extractor/extraction.lock')


_FIELDS = {
    'spark': {'connect_url':'connect_url', 'session_prefix':'session_prefix', 'connect_timeout_seconds':'connect_timeout_seconds', 'execution_timeout_seconds':'execution_timeout_seconds'},
    'hdfs': {'uri':'hdfs_uri', 'input_base':'input_base', 'output_base':'output_base'},
    'query': {k:k for k in ('max_days','max_limit','max_output_files','max_planned_rows_per_file')},
    'output': {k:k for k in ('max_records_per_file','timezone','target_compressed_mb')},
    'webhdfs': {'base_url':'webhdfs_base_url','timeout_seconds':'webhdfs_timeout_seconds','user':'webhdfs_user','token':'webhdfs_token'},
    'logging': {'file':'log_file'},
    'runtime': {'lock_file':'lock_file'},
}


def _fail(field):
    raise ExtractionError('CONFIG_ERROR', f'تنظیم مدیر نامعتبر یا وارد نشده است: {field}')


def _flatten(data):
    if not isinstance(data, dict):
        _fail('YAML')
    result = {}
    for key, value in data.items():
        if key in Config.__dataclass_fields__:
            result[key] = value
        elif key in _FIELDS and isinstance(value, dict):
            for subkey, subvalue in value.items():
                if subkey not in _FIELDS[key]:
                    _fail(key + '.' + str(subkey))
                result[_FIELDS[key][subkey]] = subvalue
        else:
            _fail(str(key))
    return result


def _validate(config, downloader):
    for field in ('max_days','max_limit','max_output_files','max_planned_rows_per_file','max_records_per_file'):
        value = getattr(config, field)
        if type(value) is not int or value < 1:
            _fail(field)
    if config.max_days > 7 or config.max_limit > 1_000_000_000:
        _fail('query limits')
    for field in ('connect_timeout_seconds','execution_timeout_seconds','webhdfs_timeout_seconds','target_compressed_mb'):
        value = getattr(config, field)
        if field == 'target_compressed_mb' and value is None:
            continue
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            _fail(field)
    for field in ('input_base','output_base'):
        value = getattr(config, field)
        if not isinstance(value, str) or not value.startswith('/') or any(c in value for c in ('\x00','\\','?','#','*','[',']','{','}')) or '..' in value.split('/'):
            _fail(field)
        if str(PurePosixPath(value)) != value.rstrip('/') and value != '/':
            _fail(field)
    for field, schemes, required in [('connect_url', ('sc',), not downloader), ('hdfs_uri', ('hdfs',), not downloader), ('webhdfs_base_url', ('http','https'), True)]:
        value = getattr(config, field)
        if value is None and not required:
            continue
        if not isinstance(value, str) or any(c.isspace() for c in value):
            _fail(field)
        try:
            parsed = urlsplit(value)
            port = parsed.port
            if parsed.scheme not in schemes or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
                _fail(field)
            if field != 'webhdfs_base_url' and parsed.path not in ('', '/'):
                _fail(field)
            if field == 'connect_url' and port is None:
                _fail(field)
        except ValueError:
            _fail(field)
    if not isinstance(config.session_prefix, str) or not config.session_prefix or not all(c.isalnum() or c in '_-' for c in config.session_prefix):
        _fail('session_prefix')
    if config.timezone != 'Asia/Tehran':
        _fail('timezone')
    try:
        ZoneInfo(config.timezone)
    except ZoneInfoNotFoundError:
        _fail('timezone')
    for field in ('webhdfs_user','webhdfs_token'):
        value = getattr(config, field)
        if value is not None and (not isinstance(value, str) or not value.strip() or any(ord(c) < 32 for c in value)):
            _fail(field)
    for field in ('log_file','lock_file'):
        value = getattr(config, field)
        if not isinstance(value, str) or not value.strip() or '\x00' in value:
            _fail(field)
    return config


def load_config(path=None, downloader=False):
    import yaml
    try:
        defaults = yaml.safe_load(files('data_extractor').joinpath('resources/defaults.yaml').read_text(encoding='utf-8'))
        values = _flatten(defaults)
        selected = path or (os.environ.get('DATA_EXTRACTOR_DOWNLOAD_CONFIG') if downloader else None) or os.environ.get('DATA_EXTRACTOR_CONFIG')
        if selected:
            values.update(_flatten(yaml.safe_load(Path(selected).read_text(encoding='utf-8'))))
        return _validate(Config(**values), downloader)
    except ExtractionError:
        raise
    except (OSError, ValueError, TypeError, yaml.YAMLError) as exc:
        raise ExtractionError('CONFIG_ERROR') from exc
