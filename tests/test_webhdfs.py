from types import SimpleNamespace
from unittest.mock import Mock

import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

from data_extractor.exceptions import ExtractionError
from data_extractor.webhdfs import WebHDFS


def client():
    value = WebHDFS(SimpleNamespace(webhdfs_base_url='http://namenode:9870',
        webhdfs_timeout_seconds=3, webhdfs_user='user', webhdfs_token=None))
    value.session = Mock()
    return value


def response(status=200, data=None, chunks=()):
    result = Mock(status_code=status)
    result.json.return_value = data
    result.iter_content.return_value = iter(chunks)
    return result


class WebHDFSTests(unittest.TestCase):
    def test_missing_only_for_file_not_found(self):
        value = client()
        value.session.request.return_value = response(404, {'RemoteException': {'exception': 'FileNotFoundException'}})
        self.assertFalse(value.exists('/missing'))
        value.session.request.return_value = response(404, {'RemoteException': {'exception': 'AccessControlException'}})
        with self.assertRaises(ExtractionError):
            value.exists('/denied')

    def test_download_validates_and_publishes(self):
        with TemporaryDirectory() as directory:
            target = Path(directory) / 'part.csv.gz'
            value = client()
            value.session.request.return_value = response(chunks=[b'ab', b'cd'])
            value.download('/result/part.csv.gz', target, 4)
            self.assertEqual(target.read_bytes(), b'abcd')
            with self.assertRaises(ExtractionError):
                value.download('/result/part.csv.gz', target, 4)
            self.assertEqual(list(Path(directory).iterdir()), [target])

    def test_size_mismatch_leaves_no_partial_file(self):
        with TemporaryDirectory() as directory:
            value = client()
            value.session.request.return_value = response(chunks=[b'bad'])
            with self.assertRaises(ExtractionError):
                value.download('/part.csv.gz', Path(directory) / 'part.csv.gz', 8)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_manifest_create_redirect_and_rename(self):
        value = client()
        redirect = response(307)
        redirect.headers = {'Location': 'http://datanode:9864/webhdfs/v1/tmp?op=CREATE'}
        redirect.url = 'http://namenode:9870/webhdfs/v1/tmp'
        value.session.request.side_effect = [redirect, response(201), response(data={'boolean': True}), response()]
        value.create_json('/result/_extract_manifest.json', {'status': 'COMPLETE'})
        calls = value.session.request.call_args_list
        self.assertEqual(calls[0].kwargs['params']['overwrite'], 'false')
        self.assertEqual(calls[1].kwargs['data'], b'{"status": "COMPLETE"}')
        self.assertEqual(calls[2].kwargs['params']['destination'], '/result/_extract_manifest.json')

    def test_manifest_upload_failure_cleans_temporary_file(self):
        value = client()
        redirect = response(307)
        redirect.headers = {'Location': 'http://datanode:9864/webhdfs/v1/tmp?op=CREATE'}
        redirect.url = 'http://namenode:9870/webhdfs/v1/webhdfs/v1/tmp'
        value.session.request.side_effect = [redirect, response(500), response()]

        with self.assertRaises(ExtractionError):
            value.create_json('/result/_extract_manifest.json', {'status': 'COMPLETE'})

        calls = value.session.request.call_args_list
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0].kwargs['params']['op'], 'CREATE')
        self.assertEqual(calls[2].args, ('DELETE', calls[0].args[1]))
        self.assertEqual(calls[2].kwargs['params']['op'], 'DELETE')

    def test_manifest_rename_false_is_failure_and_cleans_temporary_file(self):
        value = client()
        redirect = response(307)
        redirect.headers = {'Location': 'http://datanode:9864/webhdfs/v1/tmp?op=CREATE'}
        redirect.url = 'http://namenode:9870/webhdfs/v1/tmp'
        value.session.request.side_effect = [redirect, response(201), response(data={'boolean': False}), response()]

        with self.assertRaises(ExtractionError):
            value.create_json('/result/_extract_manifest.json', {'status': 'COMPLETE'})

        calls = value.session.request.call_args_list
        self.assertEqual(len(calls), 4)
        self.assertEqual(calls[2].kwargs['params']['op'], 'RENAME')
        self.assertEqual(calls[3].kwargs['params']['op'], 'DELETE')

    def test_network_timeout_is_translated(self):
        import requests
        value = client()
        value.session.request.side_effect = requests.Timeout('secret URL')
        with self.assertRaises(ExtractionError) as caught:
            value.exists('/result')
        self.assertNotIn('secret', str(caught.exception))
