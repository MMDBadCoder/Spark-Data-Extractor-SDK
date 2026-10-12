from functools import lru_cache
from importlib.resources import files


@lru_cache(maxsize=1)
def catalog():
    import yaml
    return yaml.safe_load(files('data_extractor').joinpath('resources/messages_fa.yaml').read_text(encoding='utf-8'))


def message(code, **kwargs):
    template = catalog().get(code, 'خطایی رخ داد؛ کد خطا: ' + str(code))
    return template.format(**kwargs) if kwargs else template


def emit(code, **kwargs):
    print(message(code, **kwargs), flush=True)
