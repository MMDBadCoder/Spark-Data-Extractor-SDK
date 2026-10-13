import io
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from data_extractor import ColumnInfo, Query, executor, session
from data_extractor.config import Config
from data_extractor.exceptions import ExtractionError


class _Spark:
    def __init__(self):
        self.read = Mock()
        self.frame = SimpleNamespace(schema=SimpleNamespace(fields=[
            SimpleNamespace(name='customer', dataType=SimpleNamespace(simpleString=lambda: 'string'), nullable=False),
            SimpleNamespace(name='amount', dataType=SimpleNamespace(simpleString=lambda: 'decimal(12,2)'), nullable=True),
        ]))
        self.read.parquet.return_value = self.frame
        self.stopped = False
        self.cancelled = threading.Event()

    def stop(self):
        self.stopped = True

    def interruptAll(self):
        self.cancelled.set()


class ShowColumnsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name)
        self.config = Config(log_file=str(base / 'log'), lock_file=str(base / 'lock'),
                             connect_timeout_seconds=.07, execution_timeout_seconds=.2,
                             input_base='/data/source', hdfs_uri='hdfs://namenode',
                             connect_url='sc://test:15002')
        self.query = Query('inspect').between('2026-10-01', '2026-10-02').limit(3).output_files(2)
        self.spark = _Spark()
        self.spark.range = Mock(side_effect=AssertionError('schema discovery must not scan rows'))
        self.client = Mock()
        self.client.__enter__ = Mock(return_value=self.client)
        self.client.__exit__ = Mock(return_value=False)
        self.client.exists.return_value = True
        self.client.list_status.return_value = [{'pathSuffix': 'chunk-0.parquet', 'type': 'FILE'}]
        self.session = self.spark
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(redirect_stdout(io.StringIO()))
        self.stack.enter_context(patch('data_extractor.query.load_config', return_value=self.config))
        self.stack.enter_context(patch.dict('os.environ', {'SPARK_REMOTE': 'sc://test:15002'}))
        self.stack.enter_context(patch.object(executor, 'WebHDFS', return_value=self.client))
        self.stack.enter_context(patch.object(session, 'release', side_effect=lambda _: setattr(self.session, 'stopped', True)))
        self.stack.enter_context(patch.object(session, 'create_session', return_value=self.session))
        self.stack.enter_context(patch.object(session, 'ready'))
        self.stack.enter_context(patch.object(session, 'cancel', side_effect=lambda s: s.cancelled.set()))
        self.writer = self.stack.enter_context(patch.object(executor, 'write_and_verify'))

    def test_schema_is_returned_and_printed_without_output_side_effects(self):
        result = self.query.show_columns()
        self.assertEqual(result, (ColumnInfo('customer', 'string', False),
                                  ColumnInfo('amount', 'decimal(12,2)', True)))
        self.spark.read.parquet.assert_called_once_with(
            'hdfs://namenode/data/source/2026/10/01/*.parquet',
            'hdfs://namenode/data/source/2026/10/02/*.parquet')
        self.assertEqual(self.client.exists.call_count, 2)
        self.client.create_json.assert_not_called()
        self.writer.assert_not_called()
        self.spark.range.assert_not_called()
        self.assertTrue(self.session.stopped)

    def test_dates_are_required_and_limit_and_files_are_optional(self):
        with self.assertRaises(ExtractionError) as caught:
            Query('inspect').show_columns()
        self.assertEqual(caught.exception.code, 'DATES_REQUIRED')
        query = Query('inspect').between('2026-10-01', '2026-10-01')
        self.client.list_status.return_value = [{'pathSuffix': 'chunk.parquet', 'type': 'FILE'}]
        self.assertEqual(len(query.show_columns()), 2)

    def test_each_day_must_have_parquet(self):
        self.client.list_status.side_effect = [[{'pathSuffix': 'day.parquet'}], []]
        with self.assertRaises(ExtractionError) as caught:
            self.query.show_columns()
        self.assertEqual(caught.exception.code, 'MISSING_DAY')
        self.spark.read.parquet.assert_not_called()
        self.client.create_json.assert_not_called()

    def test_directory_named_parquet_does_not_count_as_input_file(self):
        self.client.list_status.return_value = [{'pathSuffix': 'folder.parquet', 'type': 'DIRECTORY'}]
        with self.assertRaises(ExtractionError) as caught:
            self.query.show_columns()
        self.assertEqual(caught.exception.code, 'MISSING_DAY')
        self.spark.read.parquet.assert_not_called()

    def test_attached_filters_and_projection_are_ignored(self):
        self.query._predicates = ['an attached filter']
        self.query._columns = ('not_in_source',)
        self.spark.frame.filter = Mock()
        result = self.query.show_columns()
        self.assertEqual(len(result), 2)
        self.spark.read.parquet.assert_called_once()
        self.spark.frame.filter.assert_not_called()

    def test_schema_errors_are_mapped_and_session_is_released(self):
        self.spark.read.parquet.side_effect = RuntimeError('bad schema')
        with self.assertRaises(ExtractionError) as caught:
            self.query.show_columns()
        self.assertEqual(caught.exception.code, 'SCHEMA_FAILED')
        self.assertTrue(self.session.stopped)

    def test_timed_out_schema_work_keeps_lock_and_cannot_write(self):
        gate = threading.Event()
        self.spark.read.parquet.side_effect = lambda *_: (gate.wait(2), self.spark.frame)[1]
        self.config = Config(**{**self.config.__dict__, 'execution_timeout_seconds': .05})
        try:
            with self.assertRaises(ExtractionError) as caught:
                self.query.show_columns()
            self.assertEqual(caught.exception.code, 'EXECUTION_TIMEOUT')
            with self.assertRaises(ExtractionError) as busy:
                self.query.show_columns()
            self.assertEqual(busy.exception.code, 'BUSY')
        finally:
            gate.set()
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline and not self.session.stopped:
            time.sleep(.005)
        self.client.create_json.assert_not_called()


if __name__ == '__main__':
    unittest.main()
