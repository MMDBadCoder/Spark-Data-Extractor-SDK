"""Synchronous orchestration with bounded caller wait and conservative locks.

Python cannot kill a blocked gRPC call. A timed-out daemon worker retains the
SDK lock until it and session cleanup finish. It cannot publish a manifest.
"""
import fcntl
import json
import logging
import os
import posixpath
import threading
import time
import traceback
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
from zoneinfo import ZoneInfo

from . import session as sessions
from .exceptions import ExtractionError
from .messages import emit
from .result import Result
from .query import ColumnInfo
from .webhdfs import WebHDFS
from .writer import input_paths, write_and_verify

LOG = logging.getLogger('data_extractor')
_PROCESS_LOCK = threading.Lock()
_LOG_LOCK = threading.Lock()


class _JSONFormatter(logging.Formatter):
    def format(self, record):
        data = {'time': datetime.now().astimezone().isoformat(), 'level': record.levelname,
                'message': record.getMessage()}
        if record.exc_info:
            data['traceback'] = ''.join(traceback.format_exception(*record.exc_info))
        return json.dumps(data, ensure_ascii=False)


def _logging(config):
    path = Path(config.log_file).expanduser().absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _LOG_LOCK:
        if not any(getattr(h, 'baseFilename', None) == str(path) for h in LOG.handlers):
            fd = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
            os.close(fd)
            handler = logging.FileHandler(path, encoding='utf-8')
            handler.setFormatter(_JSONFormatter())
            LOG.addHandler(handler)
        LOG.setLevel(logging.INFO)
        LOG.propagate = False


class _Lease:
    def __init__(self, config):
        self.fd = None
        if not _PROCESS_LOCK.acquire(blocking=False):
            raise ExtractionError('BUSY')
        try:
            path = Path(config.lock_file).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            self.fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as exc:
            self.close()
            if isinstance(exc, BlockingIOError):
                raise ExtractionError('BUSY') from None
            raise ExtractionError('CONFIG_ERROR') from exc

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        _PROCESS_LOCK.release()


class _State:
    def __init__(self):
        self.session = None
        self.ready = threading.Event()
        self.done = threading.Event()
        self.aborted = threading.Event()
        self.caller_done = threading.Event()
        self.stop_started = threading.Event()
        self.stop_done = threading.Event()
        self.stop_lock = threading.Lock()
        self.result = None
        self.error = None
        self.ready_at = None
        self.done_at = None

    def checkpoint(self):
        if self.aborted.is_set():
            raise ExtractionError('EXECUTION_TIMEOUT')

    def stop(self):
        if self.session is None:
            return
        with self.stop_lock:
            first = not self.stop_started.is_set()
            self.stop_started.set()
        if first:
            try:
                sessions.release(self.session)
            finally:
                self.stop_done.set()
        else:
            self.stop_done.wait()


def _worker(state, lease, config, name, operation):
    try:
        state.checkpoint()
        emit('CONNECTING')
        state.session = sessions.create_session(config, name)
        state.checkpoint()
        sessions.label(state.session, name)
        state.checkpoint()
        sessions.ready(state.session)
        state.checkpoint()
        state.ready_at = time.monotonic()
        state.ready.set()
        state.result = operation(state.session, state.checkpoint)
    except BaseException as exc:
        state.error = exc
        LOG.exception('event=worker_failed remote_cleanup=unconfirmed')
        if state.session is not None:
            # Cancellation must not prevent the finally block from running.
            threading.Thread(target=sessions.cancel, args=(state.session,), daemon=True).start()
    finally:
        try:
            state.stop()
        finally:
            state.done_at = time.monotonic()
            state.done.set()
            # Success holds the lease through manifest publication by caller;
            # timeout holds it until all worker and stop operations terminate.
            state.caller_done.wait()
            lease.close()


def _execute(config, name, operation, result_handler):
    try:
        _logging(config)
    except OSError as exc:
        raise ExtractionError('CONFIG_ERROR') from exc
    emit('VALIDATING')
    state = _State()
    start = time.monotonic()
    execution_id = uuid4().hex[:12]
    name = f'{config.session_prefix}_{name}_{execution_id}'
    lease = _Lease(config)
    thread = threading.Thread(target=_worker, args=(state, lease, config, name, operation), daemon=True)
    try:
        thread.start()
    except BaseException:
        lease.close()
        raise
    try:
        while not state.done.is_set():
            elapsed = time.monotonic() - start
            if not state.ready.is_set() and elapsed >= config.connect_timeout_seconds:
                raise ExtractionError('SESSION_TIMEOUT')
            if elapsed >= config.execution_timeout_seconds:
                raise ExtractionError('EXECUTION_TIMEOUT')
            remaining = config.execution_timeout_seconds - elapsed
            if not state.ready.is_set():
                remaining = min(remaining, config.connect_timeout_seconds - elapsed)
            state.done.wait(min(0.05, max(0, remaining)))
        if state.ready_at is not None and state.ready_at - start > config.connect_timeout_seconds:
            raise ExtractionError('SESSION_TIMEOUT')
        if state.done_at - start > config.execution_timeout_seconds:
            raise ExtractionError('EXECUTION_TIMEOUT')
        if state.error is not None:
            if isinstance(state.error, ExtractionError):
                raise state.error
            code = 'WRITE_FAILED' if state.ready.is_set() else 'SESSION_FAILED'
            raise ExtractionError(code) from state.error
        return result_handler(state.result, execution_id, start)
    except BaseException:
        state.aborted.set()
        if not state.done.is_set():
            LOG.warning('event=abort execution_id=%s remote_cleanup=unknown lock_retained_until_worker_exit=true', execution_id)
            threading.Thread(target=sessions.cancel, args=(state.session,), daemon=True).start()
            threading.Thread(target=state.stop, daemon=True).start()
        LOG.exception('event=run_failed execution_id=%s', execution_id)
        raise
    finally:
        state.caller_done.set()
        if state.done.is_set():
            thread.join(timeout=0.2)


def run_query(spec, config):
    captured = datetime.now(ZoneInfo(config.timezone))
    date = captured.date().isoformat()
    output_path = posixpath.join(config.output_base, 'result', captured.strftime('%Y/%m/%d'), spec.name)

    def operation(spark, checkpoint):
        with WebHDFS(config) as hdfs:
            if hdfs.exists(output_path):
                raise ExtractionError('OUTPUT_EXISTS')
            checkpoint()
            return write_and_verify(spark, spec, config, hdfs, output_path, checkpoint)

    def finish(measured, execution_id, start):
        manifest = dict(version=1, status='COMPLETE', query_name=spec.name,
                        execution_id=execution_id, execution_date=date,
                        created_at=captured.isoformat(), **measured)
        with WebHDFS(config) as hdfs:
            hdfs.create_json(output_path + '/_extract_manifest.json', manifest)
        result = Result(spec.name, execution_id, date, time.monotonic() - start,
                        measured['row_count'], len(measured['files']), measured['total_bytes'],
                        output_path, f'data-extractor download {spec.name} --date {date} --output ./downloads')
        emit('COMPLETE')
        emit('SUMMARY', **asdict(result))
        LOG.info('event=complete execution_id=%s rows=%s files=%s', execution_id, result.row_count, result.file_count)
        return result

    return _execute(config, spec.name, operation, finish)


def discover_columns(query_name, start_date, end_date, config):
    """Read only the schemas for configured daily Parquet inputs."""
    def operation(spark, checkpoint):
        with WebHDFS(config) as hdfs:
            emit('READING')
            date_range = SimpleNamespace(start_date=start_date, end_date=end_date)
            paths = input_paths(date_range, config, hdfs)
        checkpoint()
        emit('SCHEMA_READING')
        try:
            schema = spark.read.parquet(*paths).schema
            return tuple(ColumnInfo(field.name, field.dataType.simpleString(), field.nullable)
                         for field in schema.fields)
        except ExtractionError:
            raise
        except BaseException as exc:
            raise ExtractionError('SCHEMA_FAILED') from exc

    def finish(columns, _execution_id, _start):
        _print_columns(columns)
        return columns

    return _execute(config, query_name, operation, finish)


_TYPE_LABELS = {'string': 'متن', 'boolean': 'منطقی', 'byte': 'عدد صحیح',
                'short': 'عدد صحیح', 'integer': 'عدد صحیح',
                'int': 'عدد صحیح', 'long': 'عدد صحیح',
                'bigint': 'عدد صحیح', 'float': 'عدد اعشاری',
                'double': 'عدد اعشاری', 'decimal': 'عدد اعشاری',
                'date': 'تاریخ', 'timestamp': 'زمان', 'timestamp_ntz': 'زمان بدون منطقهٔ زمانی',
                'binary': 'دادهٔ باینری', 'array': 'فهرست', 'map': 'نگاشت', 'struct': 'ساختار'}


def _print_columns(columns):
    emit('COLUMNS_TITLE')
    print('نام ستون | نوع داده | تهی‌پذیر', flush=True)
    print('---------|----------|----------', flush=True)
    for column in columns:
        label = _TYPE_LABELS.get(column.data_type.split('(')[0], column.data_type)
        print(f'{column.name} | {label} | {"بله" if column.nullable else "خیر"}', flush=True)
