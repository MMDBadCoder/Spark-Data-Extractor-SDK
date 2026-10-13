"""Bounded CSV output and exact verification of the materialized output."""
import posixpath
from datetime import timedelta
from .exceptions import ExtractionError
from .messages import emit

CSV_OPTIONS = dict(header='true', encoding='UTF-8', compression='gzip',
                   quote='"', escape='"', quoteAll='true', nullValue='\\N',
                   emptyValue='', ignoreLeadingWhiteSpace='false',
                   ignoreTrailingWhiteSpace='false')


def input_paths(spec, config, webhdfs):
    paths = []
    day = spec.start_date
    while day <= spec.end_date:
        directory = posixpath.join(config.input_base, day.strftime('%Y/%m/%d'))
        if not webhdfs.exists(directory):
            raise ExtractionError('MISSING_DAY')
        entries = webhdfs.list_status(directory)
        if not any(x.get('type') == 'FILE' and x.get('pathSuffix', '').endswith('.parquet') for x in entries):
            raise ExtractionError('MISSING_DAY')
        paths.append(config.hdfs_uri.rstrip('/') + directory + '/*.parquet')
        day += timedelta(days=1)
    return paths


def write_and_verify(session, spec, config, webhdfs, output_path, checkpoint):
    emit('READING')
    paths = input_paths(spec, config, webhdfs)
    checkpoint()
    try:
        frame = session.read.parquet(*paths)
    except Exception as exc:
        raise ExtractionError('QUERY_FAILED') from exc
    emit('FILTERING')
    try:
        for predicate in spec.predicates:
            frame = frame.filter(predicate)
        columns = frame.columns
        if len(columns) != len(set(columns)):
            raise ExtractionError('INVALID_COLUMN')
        if spec.columns:
            if any(c not in columns for c in spec.columns):
                raise ExtractionError('INVALID_COLUMN')
            # Quote identifiers: literal dots/backticks in real column names.
            frame = frame.select(*['`' + c.replace('`', '``') + '`' for c in spec.columns])
        columns = frame.columns
        if not columns:
            raise ExtractionError('INVALID_COLUMN')
        frame.schema  # Force analysis before creating an output directory.
    except ExtractionError:
        raise
    except Exception as exc:
        raise ExtractionError('QUERY_FAILED') from exc
    checkpoint()
    emit('LIMITING')
    bounded = frame.limit(spec.row_limit)
    # A global limit can collapse the result onto one partition. Repartition
    # distributes this bounded result when more than one writer is requested.
    planned = bounded.coalesce(1) if spec.file_count == 1 else bounded.repartition(spec.file_count)
    checkpoint()
    emit('WRITING')
    uri = config.hdfs_uri.rstrip('/') + output_path
    try:
        planned.write.mode('errorifexists').options(
            **CSV_OPTIONS,
            maxRecordsPerFile=str(config.max_records_per_file),
        ).csv(uri)
    except Exception as exc:
        raise ExtractionError('WRITE_FAILED') from exc
    checkpoint()
    emit('INSPECTING')
    try:
        entries = webhdfs.list_status(output_path)
        files = sorted(
            [{'name': x['pathSuffix'], 'size_bytes': x['length']} for x in entries
             if x.get('type') == 'FILE' and x['pathSuffix'].startswith('part-')
             and x['pathSuffix'].endswith('.csv.gz')],
            key=lambda x: x['name'],
        )
        if not files:
            # A missing part is ambiguous; never infer a verified empty result.
            raise ExtractionError('WRITE_FAILED')
        emit('VERIFYING')
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField(c, StringType(), True) for c in columns])
        read_options = {k: v for k, v in CSV_OPTIONS.items() if k not in ('compression', 'quoteAll')}
        read_options.update(multiLine='true', mode='FAILFAST', enforceSchema='false')
        verified = session.read.schema(schema).options(**read_options).csv(
            [uri + '/' + f['name'] for f in files]
        ).count()
    except ExtractionError:
        raise
    except Exception as exc:
        raise ExtractionError('WRITE_FAILED') from exc
    checkpoint()
    if not isinstance(verified, int) or verified < 0 or verified > spec.row_limit:
        raise ExtractionError('WRITE_FAILED')
    return {'row_count': verified, 'files': files, 'total_bytes': sum(f['size_bytes'] for f in files)}
