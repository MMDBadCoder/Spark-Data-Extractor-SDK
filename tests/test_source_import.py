import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class SourceImportTests(unittest.TestCase):
    def test_import_from_checkout_root_in_clean_subprocess(self):
        code = '''import sys, types
requests = types.ModuleType("requests")
requests.Session = object
requests.RequestException = Exception
sys.modules["requests"] = requests
import data_extractor
from pathlib import Path
assert Path(data_extractor.__file__).resolve().parent == Path.cwd() / "data_extractor"
'''
        env = dict(os.environ)
        env.pop('PYTHONPATH', None)
        subprocess.run([sys.executable, '-c', code], cwd=ROOT, env=env, check=True, capture_output=True, text=True)

    def test_source_zip_imports_resources_and_ignores_cwd_admin(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            source_zip = temp / 'source.zip'
            with zipfile.ZipFile(source_zip, 'w', zipfile.ZIP_DEFLATED) as archive:
                for path in (ROOT / 'data_extractor').rglob('*'):
                    if path.is_file() and '__pycache__' not in path.parts:
                        archive.write(path, path.relative_to(ROOT))
            (temp / 'admin.yaml').write_text(
                'spark:\n  connect_url: sc://fake:15002\nhdfs:\n  uri: hdfs://fake\nwebhdfs:\n  base_url: https://fake\n',
                encoding='utf-8',
            )
            code = '''import sys, types
sys.path.insert(0, sys.argv[1])
requests = types.ModuleType("requests")
requests.Session = object
requests.RequestException = Exception
sys.modules["requests"] = requests
import data_extractor
from importlib.resources import files
from data_extractor.config import load_config
from data_extractor.exceptions import ExtractionError
assert data_extractor.Query
assert files("data_extractor").joinpath("resources/defaults.yaml").is_file()
try:
    load_config()
except ExtractionError as error:
    assert "CONFIG_ERROR" in str(error)
else:
    raise AssertionError("ZIP import must not read cwd admin.yaml")
'''
            env = dict(os.environ)
            env.pop('PYTHONPATH', None)
            subprocess.run([sys.executable, '-c', code, str(source_zip)], cwd=temp, env=env, check=True, capture_output=True, text=True)


if __name__ == '__main__':
    unittest.main()
