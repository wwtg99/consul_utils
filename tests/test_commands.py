import os
import sys


sys.path.insert(0, os.path.abspath('lib'))
import pytest
import random
from hsettings import Settings
from consul_utils.search import ConsulKvSearch
from consul_utils.commands import CopyCommand
from consul_utils.reporter import OUT_FILTERED_KEY, OUT_FLAG_KEY


class TestCommand:

    @pytest.fixture(scope='module')
    def settings(self):
        settings = {
            'consul': {
                'host': 'test.consul.com',
                'port': 8500,
                'scheme': 'http',
                'token': '',
                'root': ''
            },
            'cache': {
                'cache_enabled': True,
                'cache_dir': '.consul_cache',
                'cache_ttl': 60,
            },
            'reporter': {
                'output_type': 'text',
                'output_file': '',
                'show_all_scan': False,
                'show_filtered': True,
                'show_flags': False
            },
            'log': {
                'log_level': 'ERROR',
                'log_formatter': '[%(levelname)s] %(asctime)s : %(message)s',
            }
        }
        return Settings(settings)

    def test_copy(self, settings):
        copy_source = 'test_copy_source_{}/source'.format(random.randint(100, 999))
        copy_target = 'test_copy_target_{}/target'.format(random.randint(100, 999))
        # add source keys
        consul = ConsulKvSearch(**dict(settings.get('consul')))
        keys = {
            'a1': 'a',
            'b1': 'b',
            'c1': 'c',
            'd1': {
                'dd1': 'dd1',
                'dd2': 'dd2'
            }
        }
        for key, val in keys.items():
            if isinstance(val, dict):
                for k, v in val.items():
                    consul.put(key='/'.join([copy_source, key, k]), value=v)
            else:
                consul.put(key='/'.join([copy_source, key]), value=val)
        args = {
            'root': copy_source,
            'target_root': copy_target
        }
        cmd = CopyCommand(settings=settings, args=args)
        res = cmd.run()
        assert OUT_FLAG_KEY in res and CopyCommand.COPY_FLAG in res[OUT_FLAG_KEY]
        assert OUT_FILTERED_KEY in res
        assert len(res[OUT_FILTERED_KEY]) == len(res[OUT_FLAG_KEY][CopyCommand.COPY_FLAG]) == 5
        # check target keys
        for key, val in keys.items():
            if isinstance(val, dict):
                for k, v in val.items():
                    res = consul.get_key(key='/'.join([copy_target, key, k]))
                    assert res is not None and len(res) == 1
                    assert res[0]['value'] == v
            else:
                res = consul.get_key(key='/'.join([copy_target, key]))
                assert res is not None and len(res) == 1
                assert res[0]['value'] == val
        # delete keys
        consul.delete(key=copy_source, recurse=True)
        consul.delete(key=copy_target, recurse=True)
