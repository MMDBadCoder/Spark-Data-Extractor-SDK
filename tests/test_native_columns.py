"""Optional real client smoke: constructs expressions without any server."""
import unittest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch
import yaml
from data_extractor import Query
from data_extractor.config import Config


class NativeColumnTests(unittest.TestCase):
    def test_native_connect_expressions_need_no_live_session(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / 'admin.yaml'
            config.write_text(yaml.safe_dump({'spark': {'connect_url':'sc://test:15002'}, 'hdfs': {'uri':'hdfs://test'}, 'webhdfs': {'base_url':'https://test'}}))
            with patch.dict(os.environ, {'DATA_EXTRACTOR_CONFIG': str(config), 'SPARK_REMOTE': ''}, clear=True):
                query = Query('native')  # configure Connect dispatch before importing/calling col
                try:
                    from pyspark.sql.functions import col
                    from pyspark.sql.connect.column import Column
                except ImportError:
                    self.skipTest('optional Spark Connect client is not installed')
                expression = (col('a') > 10) & (col('b').startswith('x') | ~(col('b') == 'y'))
                query.between('2026-01-01', '2026-01-07').where(expression).where(col('a').isNotNull()).select('a', 'b').limit(10).output_files(2)
                self.assertEqual(os.environ['SPARK_REMOTE'], 'sc://test:15002')
        spec = query._validated_spec(Config())
        self.assertIsInstance(spec.predicates[0], Column)
        self.assertEqual(len(spec.predicates), 2)
