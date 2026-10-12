"""Optional real client smoke: constructs expressions without any server."""
import unittest
from data_extractor import Query
from data_extractor.config import Config


class NativeColumnTests(unittest.TestCase):
    def test_native_connect_expressions_need_no_live_session(self):
        try:
            from pyspark.sql.functions import col
            from pyspark.sql.connect.column import Column
        except ImportError:
            self.skipTest('optional Spark Connect client is not installed')
        expression = (col('a') > 10) & (col('b').startswith('x') | ~(col('b') == 'y'))
        query = (Query('native').between('2026-01-01', '2026-01-07')
                 .where(expression).where(col('a').isNotNull())
                 .select('a', 'b').limit(10).output_files(2))
        spec = query._validated_spec(Config())
        self.assertIsInstance(spec.predicates[0], Column)
        self.assertEqual(len(spec.predicates), 2)
