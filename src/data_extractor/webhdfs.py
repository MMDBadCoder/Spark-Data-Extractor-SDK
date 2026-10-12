"""Small WebHDFS client; paths are absolute HDFS namespace paths."""
import json
import os
from pathlib import Path
from urllib.parse import quote, urljoin
from uuid import uuid4

import requests

from .exceptions import ExtractionError


class WebHDFS:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        self.session.close()

    def _request(self, method, path, op, **kwargs):
        if not isinstance(path, str) or not path.startswith('/') or '://' in path:
            raise ExtractionError('WEBHDFS', 'مسیر ذخیره‌سازی نامعتبر است.')
        params = {'op': op}
        if self.config.webhdfs_user:
            params['user.name'] = self.config.webhdfs_user
        if self.config.webhdfs_token:
            params['delegation'] = self.config.webhdfs_token
        params.update(kwargs.pop('params', {}))
        base = self.config.webhdfs_base_url.rstrip('/')
        if not base.endswith('/webhdfs/v1'):
            base += '/webhdfs/v1'
        try:
            return self.session.request(method, base + quote(path, safe='/'), params=params,
                                        timeout=self.config.webhdfs_timeout_seconds, **kwargs)
        except requests.RequestException:
            raise ExtractionError('WEBHDFS', 'ارتباط با ذخیره‌سازی برقرار نشد.') from None

    @staticmethod
    def _check(response):
        if not 200 <= response.status_code < 300:
            raise ExtractionError('WEBHDFS', 'درخواست ذخیره‌سازی ناموفق بود.')

    def exists(self, path):
        response = self._request('GET', path, 'GETFILESTATUS')
        try:
            if response.status_code == 404:
                try:
                    if response.json().get('RemoteException', {}).get('exception') == 'FileNotFoundException':
                        return False
                except (ValueError, AttributeError):
                    pass
            self._check(response)
            return True
        finally:
            response.close()

    def list_status(self, path):
        response = self._request('GET', path, 'LISTSTATUS')
        try:
            self._check(response)
            result = response.json()['FileStatuses']['FileStatus']
            if not isinstance(result, list):
                raise ValueError()
            return result
        except (ValueError, KeyError, TypeError):
            raise ExtractionError('WEBHDFS', 'پاسخ ذخیره‌سازی نامعتبر است.') from None
        finally:
            response.close()

    def read_json(self, path):
        response = self._request('GET', path, 'OPEN')
        try:
            self._check(response)
            result = response.json()
            if not isinstance(result, dict):
                raise ValueError()
            return result
        except ValueError:
            raise ExtractionError('MANIFEST', 'فایل تأیید نتیجه نامعتبر است.') from None
        finally:
            response.close()

    def create_json(self, path, data):
        temporary = path + '.tmp-' + uuid4().hex
        payload = json.dumps(data, ensure_ascii=False).encode('utf-8')
        try:
            response = self._request('PUT', temporary, 'CREATE', params={'overwrite': 'false'},
                                     allow_redirects=False)
            try:
                if response.status_code in (307, 308):
                    location = response.headers.get('Location')
                    if not location:
                        raise ExtractionError('WEBHDFS', 'نشانی انتقال ذخیره‌سازی نامعتبر است.')
                    try:
                        uploaded = self.session.request('PUT', urljoin(response.url, location), data=payload,
                                                        timeout=self.config.webhdfs_timeout_seconds)
                    except requests.RequestException:
                        raise ExtractionError('WEBHDFS', 'نوشتن فایل تأیید نتیجه ناموفق بود.') from None
                    try:
                        self._check(uploaded)
                    finally:
                        uploaded.close()
                else:
                    # WebHDFS CREATE must redirect before receiving the body.
                    raise ExtractionError('WEBHDFS', 'پاسخ ایجاد فایل ذخیره‌سازی نامعتبر است.')
            finally:
                response.close()
            renamed = self._request('PUT', temporary, 'RENAME', params={'destination': path})
            try:
                self._check(renamed)
                if renamed.json().get('boolean') is not True:
                    raise ExtractionError('WEBHDFS', 'انتشار فایل تأیید نتیجه ناموفق بود.')
            finally:
                renamed.close()
        except (ValueError, requests.RequestException):
            raise ExtractionError('WEBHDFS', 'نوشتن فایل تأیید نتیجه ناموفق بود.') from None
        finally:
            try:
                deleted = self._request('DELETE', temporary, 'DELETE', params={'recursive': 'false'})
                deleted.close()
            except ExtractionError:
                pass

    def download(self, path, target_path, expected_size, overwrite=False):
        target = Path(target_path)
        temporary = target.with_name('.' + target.name + '.tmp-' + uuid4().hex)
        response = None
        try:
            if target.exists() and not overwrite:
                raise ExtractionError('DOWNLOAD', 'فایل محلی از قبل وجود دارد؛ از --overwrite استفاده کنید.')
            target.parent.mkdir(parents=True, exist_ok=True)
            response = self._request('GET', path, 'OPEN', stream=True)
            self._check(response)
            size = 0
            with temporary.open('xb') as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        size += len(chunk)
                        if size > expected_size:
                            raise ExtractionError('DOWNLOAD', 'اندازه فایل با نتیجه تأییدشده سازگار نیست.')
                        output.write(chunk)
            if size != expected_size:
                raise ExtractionError('DOWNLOAD', 'اندازه فایل با نتیجه تأییدشده سازگار نیست.')
            if overwrite:
                os.replace(temporary, target)
            else:
                # Atomic exclusive publication, including a concurrent downloader race.
                os.link(temporary, target)
                temporary.unlink()
        except (OSError, requests.RequestException):
            raise ExtractionError('DOWNLOAD', 'دریافت فایل ناموفق بود؛ دسترسی و فضای دیسک را بررسی کنید.') from None
        finally:
            if response is not None:
                response.close()
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
