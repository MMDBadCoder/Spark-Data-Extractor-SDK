import io
import sys
import unittest
from contextlib import redirect_stdout
from datetime import date
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

from data_extractor.config import Config
from data_extractor.exceptions import ExtractionError
from data_extractor.writer import input_paths, write_and_verify


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.config = Config(hdfs_uri='hdfs://cluster', max_records_per_file=123)
        self.spec = SimpleNamespace(start_date=date(2026, 1, 1), end_date=date(2026, 1, 2),
            columns=('literal.dot', 'has`tick'), predicates=(object(), object()), row_limit=10, file_count=3)
        self.hdfs = Mock()
        self.hdfs.exists.return_value = True
        self.hdfs.list_status.side_effect = [
            [{'type': 'FILE', 'pathSuffix': 'source.parquet'}],
            [{'type': 'FILE', 'pathSuffix': 'source.parquet'}],
            [{'type': 'FILE', 'pathSuffix': 'part-z.csv.gz', 'length': 8},
             {'type': 'FILE', 'pathSuffix': '_SUCCESS', 'length': 0},
             {'type': 'FILE', 'pathSuffix': 'part-a.csv.gz', 'length': 5}]]
        self.spark = Mock()
        self.frame = Mock()
        self.frame.columns = ['literal.dot', 'has`tick']
        self.frame.filter.return_value = self.frame
        self.frame.select.return_value = self.frame
        self.spark.read.parquet.return_value = self.frame
        self.bounded = self.frame.limit.return_value
        self.planned = self.bounded.repartition.return_value
        self.planned.write.mode.return_value.options.return_value.csv.return_value = None
        self.spark.read.schema.return_value.options.return_value.csv.return_value.count.return_value = 7
        types = ModuleType('pyspark.sql.types')
        types.StructType = lambda fields: fields
        types.StructField = lambda *args: args
        types.StringType = lambda: 'string'
        self.type_patch = patch.dict(sys.modules, {'pyspark.sql.types': types})
        self.type_patch.start()
        self.addCleanup(self.type_patch.stop)
        self.stdout = redirect_stdout(io.StringIO())
        self.stdout.__enter__()
        self.addCleanup(self.stdout.__exit__, None, None, None)

    def run_writer(self):
        return write_and_verify(self.spark, self.spec, self.config, self.hdfs, '/warehouse/result/output', Mock())

    def test_write_is_bounded_and_verifies_materialized_output(self):
        measured = self.run_writer()
        self.spark.read.parquet.assert_called_once_with(
            'hdfs://cluster/warehouse/source/2026/01/01/*.parquet',
            'hdfs://cluster/warehouse/source/2026/01/02/*.parquet')
        self.assertEqual(self.frame.filter.call_count, 2)
        self.frame.select.assert_called_once_with('`literal.dot`', '`has``tick`')
        self.frame.limit.assert_called_once_with(10)
        self.bounded.repartition.assert_called_once_with(3)
        self.bounded.coalesce.assert_not_called()
        self.planned.write.mode.assert_called_once_with('errorifexists')
        options = self.planned.write.mode.return_value.options.call_args.kwargs
        self.assertEqual(options['compression'], 'gzip')
        self.assertEqual(options['maxRecordsPerFile'], '123')
        self.assertEqual(measured, {'row_count': 7, 'files': [
            {'name': 'part-a.csv.gz', 'size_bytes': 5}, {'name': 'part-z.csv.gz', 'size_bytes': 8}], 'total_bytes': 13})
        self.spark.read.schema.return_value.options.return_value.csv.assert_called_once_with([
            'hdfs://cluster/warehouse/result/output/part-a.csv.gz',
            'hdfs://cluster/warehouse/result/output/part-z.csv.gz'])
        read_options = self.spark.read.schema.return_value.options.call_args.kwargs
        self.assertEqual(read_options['mode'], 'FAILFAST')
        self.assertEqual(read_options['multiLine'], 'true')

    def test_one_target_file_uses_coalesce(self):
        self.spec.file_count = 1
        self.run_writer()
        self.bounded.coalesce.assert_called_once_with(1)
        self.bounded.repartition.assert_not_called()

    def test_missing_day_rejects_before_spark_read(self):
        self.hdfs.exists.side_effect = [True, False]
        with self.assertRaises(ExtractionError) as caught:
            self.run_writer()
        self.assertEqual(caught.exception.code, 'MISSING_DAY')
        self.spark.read.parquet.assert_not_called()

    def test_day_without_parquet_is_missing(self):
        self.hdfs.list_status.side_effect = [[{'type': 'DIRECTORY', 'pathSuffix': 'x.parquet'}]]
        with self.assertRaises(ExtractionError) as caught:
            input_paths(self.spec, self.config, self.hdfs)
        self.assertEqual(caught.exception.code, 'MISSING_DAY')

    def test_verified_output_over_limit_rejects_result(self):
        self.spark.read.schema.return_value.options.return_value.csv.return_value.count.return_value = 11
        with self.assertRaises(ExtractionError) as caught:
            self.run_writer()
        self.assertEqual(caught.exception.code, 'WRITE_FAILED')

    def test_invalid_projection_never_writes(self):
        self.spec.columns = ('missing',)
        with self.assertRaises(ExtractionError) as caught:
            self.run_writer()
        self.assertEqual(caught.exception.code, 'INVALID_COLUMN')
        self.frame.limit.assert_not_called()

    def test_no_actual_part_file_never_infers_success(self):
        self.hdfs.list_status.side_effect = [
            [{'type': 'FILE', 'pathSuffix': 'source.parquet'}],
            [{'type': 'FILE', 'pathSuffix': 'source.parquet'}],
            [{'type': 'FILE', 'pathSuffix': '_SUCCESS', 'length': 0}]]
        with self.assertRaises(ExtractionError) as caught:
            self.run_writer()
        self.assertEqual(caught.exception.code, 'WRITE_FAILED')
        self.spark.read.schema.assert_not_called()
