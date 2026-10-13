import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import yaml
from data_extractor.config import load_config
import data_extractor.config as config_module
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

    def test_source_checkout_admin_fallback_and_environment_precedence(self):
        checkout = self.path.parent / 'checkout'
        package = checkout / 'data_extractor'
        package.mkdir(parents=True)
        admin = checkout / 'admin.yaml'
        admin.write_text(yaml.safe_dump(dict(spark={'connect_url':'sc://checkout:15002'}, hdfs={'uri':'hdfs://checkout'}, webhdfs={'base_url':'https://checkout'})))
        with patch.object(config_module, '__file__', str(package / 'config.py')), patch.dict(os.environ, {}, clear=True):
            self.assertEqual(load_config().connect_url, 'sc://checkout:15002')
            env = self.write(connect_url='sc://environment:15002')
            with patch.dict(os.environ, {'DATA_EXTRACTOR_CONFIG': str(env)}):
                self.assertEqual(load_config().connect_url, 'sc://environment:15002')

    def test_query_initializes_connect_remote_before_column_import(self):
        config_file = self.write()
        with patch.dict(os.environ, {'DATA_EXTRACTOR_CONFIG': str(config_file), 'SPARK_REMOTE': ''}, clear=True):
            from data_extractor import Query
            Query('configured')
            self.assertEqual(os.environ['SPARK_REMOTE'], 'sc://spark:15002')

    def test_query_rejects_conflicting_connect_remote(self):
        config_file = self.write()
        with patch.dict(os.environ, {'DATA_EXTRACTOR_CONFIG': str(config_file), 'SPARK_REMOTE': 'sc://other:15002'}, clear=True):
            from data_extractor import Query
            with self.assertRaisesRegex(ExtractionError, 'نشانی Spark فعال'):
                Query('configured')

    def test_explicit_path_precedes_environment_path(self):
        explicit = self.write(max_output_files=11)
        env_path = self.path.parent / 'env.yaml'
        env_path.write_text(yaml.safe_dump(dict(connect_url='sc://other:15002', hdfs_uri='hdfs://other', webhdfs_base_url='https://other', max_output_files=12)))
        with patch.dict(os.environ, {'DATA_EXTRACTOR_CONFIG': str(env_path)}):
            config = load_config(explicit)
        self.assertEqual(config.max_output_files, 11)
        self.assertEqual(config.connect_url, 'sc://spark:15002')

    def test_downloader_does_not_read_runtime_checkout_config(self):
        self.path.write_text('webhdfs:\n  base_url: https://webhdfs.example\n')
        with patch.dict(os.environ, {'DATA_EXTRACTOR_CONFIG': str(self.path)}, clear=True), self.assertRaises(ExtractionError):
            load_config(downloader=True)

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
