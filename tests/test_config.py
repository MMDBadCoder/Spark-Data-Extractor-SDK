import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import yaml
from data_extractor.config import load_config
from data_extractor.exceptions import ExtractionError


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'config.yaml'

    def write(self, **values):
        defaults = dict(connect_url='sc://spark:15002', hdfs_uri='hdfs://nn', webhdfs_base_url='https://webhdfs.example')
        defaults.update(values)
        self.path.write_text(yaml.safe_dump(defaults))
        return self.path

    def test_external_defaults(self):
        config = load_config(self.write(max_output_files=10))
        self.assertEqual(config.max_output_files, 10)
        self.assertEqual(config.max_days, 7)

    def test_invalid_configuration(self):
        for field, value in [('max_days',True), ('max_days',8), ('max_limit',1_000_000_001), ('execution_timeout_seconds',False), ('execution_timeout_seconds',float('nan')), ('input_base','/a/../b'), ('input_base','/a/*'), ('input_base','relative'), ('timezone','UTC'), ('max_output_files',0), ('connect_url','sc://spark')]:
            with self.subTest(field=field,value=value), self.assertRaises(ExtractionError):
                load_config(self.write(**{field:value}))

    def test_missing_endpoints(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(ExtractionError, 'connect_url'):
            load_config()

    def test_downloader_independent(self):
        self.path.write_text('webhdfs:\n  base_url: https://webhdfs.example\n  user: analyst\n  token: secret\n')
        with patch.dict(os.environ, {'DATA_EXTRACTOR_DOWNLOAD_CONFIG':str(self.path)}):
            config = load_config(downloader=True)
        self.assertIsNone(config.connect_url)
        self.assertEqual(config.webhdfs_user, 'analyst')

    def test_secret_not_in_error(self):
        self.write(webhdfs_base_url='https://user:secret@host')
        with self.assertRaises(ExtractionError) as error:
            load_config(self.path, downloader=True)
        self.assertNotIn('secret', str(error.exception))
