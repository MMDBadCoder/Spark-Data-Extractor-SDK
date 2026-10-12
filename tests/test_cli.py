from types import SimpleNamespace
from unittest.mock import Mock, patch

import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from contextlib import redirect_stdout
from io import StringIO

from data_extractor import cli
from data_extractor.exceptions import ExtractionError


def manifest():
    return {'version': 1, 'status': 'COMPLETE', 'query_name': 'search',
            'execution_id': 'abc', 'execution_date': '2026-01-07',
            'created_at': '2026-01-07T10:00:00+03:30', 'row_count': 1,
            'files': [{'name': 'part-00000.csv.gz', 'size_bytes': 4}], 'total_bytes': 4}


class CLITests(unittest.TestCase):
    def test_rejects_invalid_manifests(self):
        changes = [{'status': 'PARTIAL'}, {'query_name': 'other'}, {'execution_date': '2026-01-08'},
            {'total_bytes': 5}, {'row_count': True}, {'version': True},
            {'files': [{'name': '../part-00000.csv.gz', 'size_bytes': 4}]},
            {'files': [{'name': 'part-00000.csv.gz', 'size_bytes': -1}]},
            {'files': [{'name': 'part-00000.csv.gz', 'size_bytes': 2}] * 2}]
        for change in changes:
            with self.subTest(change=change):
                value = manifest()
                value.update(change)
                with self.assertRaises(ExtractionError):
                    cli.validate_manifest(value, 'search', '2026-01-07')

    def test_cli_resolves_path_and_passes_overwrite(self):
        client = Mock()
        client.read_json.return_value = manifest()
        client.__enter__ = Mock(return_value=client)
        client.__exit__ = Mock(return_value=False)
        with TemporaryDirectory() as directory, patch.object(cli, 'load_config', return_value=SimpleNamespace(output_base='/warehouse')) as loader, patch.object(cli, 'WebHDFS', return_value=client), redirect_stdout(StringIO()):
            self.assertEqual(cli.main(['download', 'search', '--date', '2026-01-07', '--output', directory, '--overwrite', '--config', 'admin.yaml']), 0)
            loader.assert_called_once_with('admin.yaml', downloader=True)
            client.read_json.assert_called_once_with('/warehouse/result/2026/01/07/search/_extract_manifest.json')
            client.download.assert_called_once_with('/warehouse/result/2026/01/07/search/part-00000.csv.gz', Path(directory) / 'search_2026-01-07' / 'part-00000.csv.gz', 4, overwrite=True)

    def test_bad_arguments_have_nonzero_persian_error(self):
        captured = StringIO()
        with redirect_stdout(captured):
            self.assertEqual(cli.main(['download', '../escape', '--date', '2026-01-07', '--output', '.']), 1)
            self.assertEqual(cli.main(['download', 'search']), 1)
        self.assertIn('NAME', captured.getvalue())
        self.assertIn('DOWNLOAD', captured.getvalue())
