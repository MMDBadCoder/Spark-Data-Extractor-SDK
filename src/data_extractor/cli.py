"""Standalone downloader entry point (does not require a Spark session)."""
import argparse
from datetime import date, datetime
from pathlib import Path
import re

from .config import load_config
from .exceptions import ExtractionError
from .webhdfs import WebHDFS

NAME = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$')
PART = re.compile(r'^part-[A-Za-z0-9_.-]+\.csv\.gz$')


def validate_manifest(manifest, name, execution_date):
    def fail():
        raise ExtractionError('MANIFEST', 'فایل تأیید نتیجه کامل یا معتبر نیست.')
    if not isinstance(manifest, dict):
        fail()
    if type(manifest.get('version')) is not int or manifest['version'] != 1:
        fail()
    if manifest.get('status') != 'COMPLETE' or manifest.get('query_name') != name or manifest.get('execution_date') != execution_date:
        fail()
    if not isinstance(manifest.get('execution_id'), str) or not manifest['execution_id'].strip():
        fail()
    try:
        created = datetime.fromisoformat(manifest['created_at'].replace('Z', '+00:00'))
        if created.tzinfo is None:
            fail()
    except (KeyError, ValueError, TypeError, AttributeError):
        fail()
    for field in ('row_count', 'total_bytes'):
        if type(manifest.get(field)) is not int or manifest[field] < 0:
            fail()
    files = manifest.get('files')
    if not isinstance(files, list):
        fail()
    names = set()
    total = 0
    for entry in files:
        if not isinstance(entry, dict):
            fail()
        filename = entry.get('name')
        if not isinstance(filename, str) or not PART.fullmatch(filename) or filename in names:
            fail()
        size = entry.get('size_bytes')
        if type(size) is not int or size < 0:
            fail()
        names.add(filename)
        total += size
    if total != manifest['total_bytes'] or (manifest['row_count'] > 0 and not files):
        fail()
    return files


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ExtractionError('DOWNLOAD', 'آرگومان‌های فرمان دریافت نامعتبر یا ناقص هستند.')


def main(argv=None):
    parser = _Parser(prog='data-extractor')
    commands = parser.add_subparsers(dest='command', required=True, parser_class=_Parser)
    command = commands.add_parser('download')
    command.add_argument('name')
    command.add_argument('--date', required=True)
    command.add_argument('--output', required=True)
    command.add_argument('--overwrite', action='store_true')
    command.add_argument('--config')
    try:
        args = parser.parse_args(argv)
        if not NAME.fullmatch(args.name):
            raise ExtractionError('NAME', 'نام پرس‌وجو نامعتبر است.')
        try:
            parsed = date.fromisoformat(args.date)
            if parsed.isoformat() != args.date:
                raise ValueError()
        except ValueError:
            raise ExtractionError('DATE', 'تاریخ باید به صورت YYYY-MM-DD باشد.') from None
        config = load_config(args.config, downloader=True)
        remote = config.output_base.rstrip('/') + '/result/' + parsed.strftime('%Y/%m/%d') + '/' + args.name
        destination = Path(args.output) / (args.name + '_' + args.date)
        with WebHDFS(config) as client:
            manifest = client.read_json(remote + '/_extract_manifest.json')
            files = validate_manifest(manifest, args.name, args.date)
            if not args.overwrite and any((destination / item['name']).exists() for item in files):
                raise ExtractionError('DOWNLOAD', 'فایل محلی از قبل وجود دارد؛ از --overwrite استفاده کنید.')
            for item in files:
                print('در حال دریافت: ' + item['name'])
                client.download(remote + '/' + item['name'], destination / item['name'], item['size_bytes'], overwrite=args.overwrite)
        print(f'دریافت کامل شد: {len(files)} فایل، {manifest["total_bytes"]} بایت، {destination}')
        return 0
    except ExtractionError as exc:
        print(str(exc))
        return 1
    except OSError:
        print('[DOWNLOAD] دسترسی به فایل محلی ناموفق بود.')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
