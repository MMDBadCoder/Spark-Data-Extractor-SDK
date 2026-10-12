import io
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from data_extractor import executor, session
from data_extractor.config import Config
from data_extractor.exceptions import ExtractionError


class FakeSession:
    def __init__(self):
        self.stopped = False
        self.cancelled = threading.Event()

    def stop(self):
        self.stopped = True

    def interruptAll(self):
        self.cancelled.set()

    def range(self, _):
        if self.stopped:
            raise RuntimeError('session already released')
        return SimpleNamespace(count=lambda: 1)


class ExecutorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        base = Path(self.directory.name)
        self.config = Config(log_file=str(base / 'log'), lock_file=str(base / 'lock'),
                             connect_timeout_seconds=.06, execution_timeout_seconds=.15)
        self.spec = SimpleNamespace(name='search')
        self.sessions = []
        self.client = Mock()
        self.client.exists.return_value = False
        self.client.__enter__ = Mock(return_value=self.client)
        self.client.__exit__ = Mock(return_value=False)
        self.measured = {'row_count': 2, 'files': [{'name': 'part-0.csv.gz', 'size_bytes': 10}], 'total_bytes': 10}
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(redirect_stdout(io.StringIO()))
        self.stack.enter_context(patch.object(executor, 'WebHDFS', return_value=self.client))
        self.stack.enter_context(patch.object(session, 'create_session', side_effect=self.new_session))
        self.writer = self.stack.enter_context(patch.object(executor, 'write_and_verify', return_value=self.measured))

    def new_session(self, *_):
        value = FakeSession()
        self.sessions.append(value)
        return value

    def await_unlock(self):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if executor._PROCESS_LOCK.acquire(blocking=False):
                executor._PROCESS_LOCK.release()
                return
            time.sleep(.002)
        self.fail('worker did not release SDK lock')

    def test_success_has_fresh_sessions_and_releases_each(self):
        first = executor.run_query(self.spec, self.config)
        second = executor.run_query(self.spec, self.config)
        self.assertEqual(first.row_count, 2)
        self.assertNotEqual(first.execution_id, second.execution_id)
        self.assertEqual(len(self.sessions), 2)
        self.assertIsNot(self.sessions[0], self.sessions[1])
        for value in self.sessions:
            self.assertTrue(value.stopped)
            with self.assertRaises(RuntimeError):
                value.range(1)
        self.assertEqual(self.client.create_json.call_count, 2)
        self.assertEqual(self.client.create_json.call_args.args[1]['status'], 'COMPLETE')

    def test_writer_error_releases_session_and_never_publishes(self):
        self.writer.side_effect = RuntimeError('Spark failed')
        with self.assertRaises(ExtractionError) as caught:
            executor.run_query(self.spec, self.config)
        self.assertEqual(caught.exception.code, 'WRITE_FAILED')
        self.assertTrue(self.sessions[0].stopped)
        self.client.create_json.assert_not_called()
        self.await_unlock()

    def test_existing_output_stops_before_writer(self):
        self.client.exists.return_value = True
        with self.assertRaises(ExtractionError) as caught:
            executor.run_query(self.spec, self.config)
        self.assertEqual(caught.exception.code, 'OUTPUT_EXISTS')
        self.writer.assert_not_called()
        self.client.create_json.assert_not_called()
        self.assertTrue(self.sessions[0].stopped)

    def test_readiness_timeout_holds_lock_until_readiness_returns(self):
        gate = threading.Event()
        with patch.object(session, 'ready', side_effect=lambda _: gate.wait(2)):
            try:
                with self.assertRaises(ExtractionError) as caught:
                    executor.run_query(self.spec, self.config)
                self.assertEqual(caught.exception.code, 'SESSION_TIMEOUT')
                with self.assertRaises(ExtractionError) as busy:
                    executor.run_query(self.spec, self.config)
                self.assertEqual(busy.exception.code, 'BUSY')
            finally:
                gate.set()
                self.await_unlock()
        self.writer.assert_not_called()
        self.client.create_json.assert_not_called()
        self.assertTrue(self.sessions[0].stopped)

    def test_execution_timeout_late_write_cannot_publish_complete(self):
        gate = threading.Event()
        def slow_write(*_):
            gate.wait(2)
            return self.measured
        self.writer.side_effect = slow_write
        try:
            with self.assertRaises(ExtractionError) as caught:
                executor.run_query(self.spec, self.config)
            self.assertEqual(caught.exception.code, 'EXECUTION_TIMEOUT')
            with self.assertRaises(ExtractionError) as busy:
                executor.run_query(self.spec, self.config)
            self.assertEqual(busy.exception.code, 'BUSY')
            self.assertTrue(self.sessions[0].cancelled.wait(1))
        finally:
            gate.set()
            self.await_unlock()
        self.client.create_json.assert_not_called()
        self.assertTrue(self.sessions[0].stopped)
