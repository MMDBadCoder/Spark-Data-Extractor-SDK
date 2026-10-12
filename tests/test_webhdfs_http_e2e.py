"""Local HTTP integration tests for the real WebHDFS client and download CLI.

The server below emulates the WebHDFS NameNode/DataNode HTTP protocol. It does
not emulate Hadoop storage semantics or Spark, and must not be described as a
cluster E2E test.
"""
import gzip
import io
import json
import csv
import posixpath
import threading
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from data_extractor import cli
from data_extractor import Query, query as query_module, session as sessions
from data_extractor.config import Config
from data_extractor.webhdfs import WebHDFS


class WebHDFSEmulator:
    """Minimal local NameNode/DataNode implementation backed by a dict."""

    def __init__(self):
        self.files = {}
        self.calls = []
        self.server = None
        self.thread = None

    def __enter__(self):
        emulator = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *_):
                pass

            def send(self, status, body=b'', headers=None):
                self.send_response(status)
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                if body:
                    self.wfile.write(body)

            def do_GET(self):
                parsed = urlparse(self.path)
                params = parse_qs(parsed.query)
                if parsed.path.startswith('/datanode'):
                    path = parsed.path.removeprefix('/datanode')
                    emulator.calls.append(('DATANODE_GET', path, 'OPEN'))
                    return self.send(200, emulator.files.get(path, b''))
                path = parsed.path.removeprefix('/webhdfs/v1')
                op = params.get('op', [''])[0]
                emulator.calls.append(('GET', path, op))
                if op == 'GETFILESTATUS':
                    if path not in emulator.files and not any(item.startswith(path.rstrip('/') + '/') for item in emulator.files):
                        return self.send(404, json.dumps({'RemoteException': {'exception': 'FileNotFoundException'}}).encode())
                    return self.send(200, b'{}')
                if op == 'LISTSTATUS':
                    prefix = path.rstrip('/') + '/'
                    names = sorted(p[len(prefix):] for p in emulator.files if p.startswith(prefix) and '/' not in p[len(prefix):])
                    statuses = [{'pathSuffix': name, 'type': 'FILE', 'length': len(emulator.files[prefix + name])} for name in names]
                    return self.send(200, json.dumps({'FileStatuses': {'FileStatus': statuses}}).encode())
                if op == 'OPEN':
                    if path not in emulator.files:
                        return self.send(404, b'not found')
                    location = f'http://127.0.0.1:{self.server.server_port}/datanode{path}?op=OPEN'
                    return self.send(307, headers={'Location': location})
                return self.send(400, b'bad op')

            def do_DELETE(self):
                parsed = urlparse(self.path)
                path = parsed.path.removeprefix('/webhdfs/v1')
                emulator.calls.append(('DELETE', path, parse_qs(parsed.query).get('op', [''])[0]))
                emulator.files.pop(path, None)
                return self.send(200, b'{"boolean":true}')

            def do_PUT(self):
                parsed = urlparse(self.path)
                params = parse_qs(parsed.query)
                if parsed.path.startswith('/datanode'):
                    path = parsed.path.removeprefix('/datanode')
                    size = int(self.headers.get('Content-Length', '0'))
                    emulator.files[path] = self.rfile.read(size)
                    emulator.calls.append(('DATANODE_PUT', path, len(emulator.files[path])))
                    return self.send(201)
                path = parsed.path.removeprefix('/webhdfs/v1')
                op = params.get('op', [''])[0]
                emulator.calls.append(('PUT', path, op))
                if op == 'CREATE':
                    location = f'http://127.0.0.1:{self.server.server_port}/datanode{path}?op=CREATE'
                    return self.send(307, headers={'Location': location})
                if op == 'RENAME':
                    target = params.get('destination', [''])[0]
                    if path not in emulator.files or target in emulator.files:
                        return self.send(200, b'{"boolean":false}')
                    emulator.files[target] = emulator.files.pop(path)
                    return self.send(200, b'{"boolean":true}')
                return self.send(400, b'bad op')

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    @property
    def base_url(self):
        return f'http://127.0.0.1:{self.server.server_port}'

    def config(self):
        return SimpleNamespace(webhdfs_base_url=self.base_url, webhdfs_timeout_seconds=2,
                               webhdfs_user=None, webhdfs_token=None, output_base='/warehouse')


class FakeSparkFrame:
    """Small Spark boundary double that writes real CSV.gz bytes."""

    def __init__(self, rows, columns, server):
        self.rows, self.columns, self.server = rows, columns, server
        self.schema = object()
        self.write = self

    def filter(self, _predicate):
        return self

    def select(self, *columns):
        names = [name.strip('`').replace('``', '`') for name in columns]
        indexes = [self.columns.index(name) for name in names]
        return FakeSparkFrame([[row[i] for i in indexes] for row in self.rows], names, self.server)

    def limit(self, count):
        return FakeSparkFrame(self.rows[:count], self.columns, self.server)

    def coalesce(self, _count):
        return self

    def repartition(self, _count):
        return self

    def mode(self, _mode):
        return self

    def options(self, **_options):
        return self

    def csv(self, uri):
        path = uri.removeprefix('hdfs://test-cluster')
        stream = io.StringIO(newline='')
        writer = csv.writer(stream, quoting=csv.QUOTE_ALL, lineterminator='\n', escapechar='"', doublequote=True)
        writer.writerow(self.columns)
        for row in self.rows:
            writer.writerow(['\\N' if value is None else value for value in row])
        self.server.files[path + '/part-00000.csv.gz'] = gzip.compress(stream.getvalue().encode('utf-8'))

    def count(self):
        return len(self.rows)


class FakeSparkReader:
    def __init__(self, server, rows, columns):
        self.server, self.rows, self.columns = server, rows, columns
        self.options_map = {}

    def parquet(self, *_paths):
        return FakeSparkFrame(self.rows, self.columns, self.server)

    def schema(self, _schema):
        return self

    def options(self, **options):
        self.options_map = options
        return self

    def csv(self, paths):
        rows = []
        for uri in paths:
            path = uri.removeprefix('hdfs://test-cluster')
            content = gzip.decompress(self.server.files[path]).decode('utf-8')
            parsed = list(csv.reader(io.StringIO(content, newline='')))
            # Header is present in the Spark-style CSV output.
            rows.extend([row for row in parsed[1:] if row])
        return FakeSparkFrame(rows, self.columns, self.server)


class FakeSparkSession:
    def __init__(self, server, rows, columns):
        self.read = FakeSparkReader(server, rows, columns)
        self.stopped = False

    def range(self, _count):
        return self

    def count(self):
        return 1

    def stop(self):
        self.stopped = True

    def interruptAll(self):
        pass


class WebHDFSHTTPE2ETests(unittest.TestCase):
    def test_public_query_full_flow_with_simulated_spark_and_real_http_cli(self):
        columns = ['id', 'note', 'blank', 'missing']
        rows = [[1, 'سلام\nجهان', '', None], [2, 'second', '', None]]
        today = datetime.now(ZoneInfo('Asia/Tehran')).date()
        day_path = today.strftime('%Y/%m/%d')
        output_date = today.isoformat()
        query_name = 'layered'
        with WebHDFSEmulator() as server, TemporaryDirectory() as temp:
            config = Config(connect_url='sc://fake-host:15002', hdfs_uri='hdfs://test-cluster',
                            input_base='/warehouse/source', output_base='/warehouse',
                            webhdfs_base_url=server.base_url, webhdfs_timeout_seconds=2,
                            connect_timeout_seconds=2, execution_timeout_seconds=5,
                            log_file=str(Path(temp) / 'extract.log'), lock_file=str(Path(temp) / 'extract.lock'))
            source_dir = f'/warehouse/source/{day_path}'
            server.files[source_dir + '/source.parquet'] = b'not-real-parquet-boundary-double'
            spark = FakeSparkSession(server, rows, columns)
            with patch.object(query_module, 'load_config', return_value=config), \
                 patch.object(sessions, 'create_session', return_value=spark), \
                 redirect_stdout(io.StringIO()) as query_output:
                result = (Query(query_name).between(output_date, output_date)
                          .select(*columns).limit(2).output_files(1).run())
            self.assertTrue(spark.stopped)
            self.assertEqual(result.row_count, 2)
            self.assertEqual(result.file_count, 1)
            self.assertIn('استخراج با موفقیت تکمیل شد.', query_output.getvalue())
            output_dir = f'/warehouse/result/{day_path}/{query_name}'
            self.assertIn(output_dir + '/_extract_manifest.json', server.files)
            part_path = output_dir + '/part-00000.csv.gz'
            expected_bytes = server.files[part_path]

            with patch.object(cli, 'load_config', return_value=config), redirect_stdout(io.StringIO()) as cli_output:
                code = cli.main(['download', query_name, '--date', output_date, '--output', temp])
            self.assertEqual(code, 0, cli_output.getvalue())
            downloaded = Path(temp) / f'{query_name}_{output_date}' / 'part-00000.csv.gz'
            self.assertEqual(downloaded.read_bytes(), expected_bytes)
            decoded = gzip.decompress(downloaded.read_bytes()).decode('utf-8')
            self.assertIn('سلام\nجهان', decoded)
            self.assertIn('""', decoded)
            self.assertIn('"\\N"', decoded)
            self.assertEqual(len(list(csv.reader(io.StringIO(decoded, newline='')))), 3)
            self.assertTrue(any(call[0] == 'DATANODE_PUT' for call in server.calls))

    def test_real_client_manifest_publish_and_cli_download(self):
        payload = gzip.compress('id,name\n1,آزمایش\n'.encode('utf-8'))
        name = 'search'
        run_date = '2026-01-07'
        result_dir = '/warehouse/result/2026/01/07/search'
        part = 'part-00000.csv.gz'
        with WebHDFSEmulator() as server, TemporaryDirectory() as temp:
            config = server.config()
            server.files[result_dir + '/' + part] = payload
            manifest = {'version': 1, 'status': 'COMPLETE', 'query_name': name,
                        'execution_id': 'local-http-1', 'execution_date': run_date,
                        'created_at': '2026-01-07T10:00:00+03:30', 'row_count': 1,
                        'files': [{'name': part, 'size_bytes': len(payload)}],
                        'total_bytes': len(payload)}
            with WebHDFS(config) as client:
                client.create_json(result_dir + '/_extract_manifest.json', manifest)
                self.assertTrue(client.exists(result_dir + '/' + part))

            # Exercise the installed CLI entry logic with only external config
            # loading supplied; WebHDFS itself goes over local HTTP throughout.
            with patch.object(cli, 'load_config', return_value=config), redirect_stdout(io.StringIO()) as out:
                code = cli.main(['download', name, '--date', run_date, '--output', temp])
            self.assertEqual(code, 0, out.getvalue())
            local = Path(temp) / f'{name}_{run_date}' / part
            self.assertEqual(local.read_bytes(), payload)
            self.assertEqual(gzip.decompress(local.read_bytes()).decode('utf-8'), 'id,name\n1,آزمایش\n')
            self.assertTrue(any(call[0] == 'GET' and call[2] == 'OPEN' for call in server.calls))
            self.assertTrue(any(call[0] == 'DATANODE_PUT' for call in server.calls))

    def test_http_cli_refuses_incomplete_manifest(self):
        with WebHDFSEmulator() as server, TemporaryDirectory() as temp:
            result_dir = '/warehouse/result/2026/01/07/search'
            manifest = {'version': 1, 'status': 'PARTIAL', 'query_name': 'search',
                        'execution_id': 'x', 'execution_date': '2026-01-07',
                        'created_at': '2026-01-07T10:00:00+03:30', 'row_count': 0,
                        'files': [], 'total_bytes': 0}
            server.files[result_dir + '/_extract_manifest.json'] = json.dumps(manifest).encode()
            config = server.config()
            with patch.object(cli, 'load_config', return_value=config), redirect_stdout(io.StringIO()) as out:
                code = cli.main(['download', 'search', '--date', '2026-01-07', '--output', temp])
            self.assertEqual(code, 1)
            self.assertIn('MANIFEST', out.getvalue())
            self.assertEqual(list(Path(temp).iterdir()), [])


if __name__ == '__main__':
    unittest.main()
