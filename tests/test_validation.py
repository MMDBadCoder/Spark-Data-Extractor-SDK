import unittest
import os
from datetime import date
from data_extractor import Query, ExtractionError
from data_extractor.config import Config
from data_extractor.validation import validate_name, parse_iso_date


class ValidationTests(unittest.TestCase):
    def test_connect_function_dispatch_enabled(self):
        self.assertEqual(os.environ.get("SPARK_CONNECT_MODE_ENABLED"), "1")

    def test_names(self):
        for name in ['', '/tmp', 'a b', '_a', 'a'*65, None]:
            with self.subTest(name=name), self.assertRaises(ExtractionError):
                validate_name(name)
        self.assertEqual(validate_name('a-b_2'), 'a-b_2')

    def test_iso_dates(self):
        for value in ['2026-2-01', '2026-02-30', '20260101', None, date(2026,1,1)]:
            with self.subTest(value=value), self.assertRaises(ExtractionError):
                parse_iso_date(value)

    def test_required_fields_and_days(self):
        q = Query('example').between('2026-01-01','2026-01-07').limit(20).output_files(2)
        self.assertEqual(q._validated_spec(Config()).row_limit,20)
        with self.assertRaises(ExtractionError):
            q.between('2026-01-01','2026-01-08')
        with self.assertRaises(ExtractionError):
            Query('example')._validated_spec(Config())

    def test_integer_controls(self):
        for value in [True,False,0,-1,1.5,'1']:
            for method in ['limit','output_files']:
                with self.subTest(value=value,method=method), self.assertRaises(ExtractionError):
                    getattr(Query('example'),method)(value)

    def test_projection_and_planning(self):
        q = Query('example').between('2026-01-01','2026-01-01').limit(100).output_files(2)
        with self.assertRaisesRegex(ExtractionError,'5'):
            q._validated_spec(Config(max_planned_rows_per_file=20))
        self.assertIsNone(q.select()._columns)
        for columns in [('a','a'),('',),(['a'],)]:
            with self.assertRaises(ExtractionError):
                q.select(*columns)
        with self.assertRaises(ExtractionError):
            q.where('a > 1')
