import os
import sys


sys.path.insert(0, os.path.abspath('lib'))
import pytest
from hsettings import Settings
from consul_utils.search import ConsulKvSearch
from consul_utils.filters import OneFilter, PairedFilter, SkipDirectoryFilter, SearchFilter, DiffFilter


class TestSearch:

    @pytest.fixture(scope='module')
    def config(self):
        config = {
            'host': 'test.consul.com',
            'port': 8500,
            'scheme': 'http',
            'token': '',
            'cache_enabled': True,
            'cache_dir': '.consul_cache',
            'cache_ttl': 60,
            'root': ''
        }
        return config

    def test_cache(self, config):
        search = ConsulKvSearch(**config)
        key = 'test/test1'
        def_val = 'haha'
        search.del_cache(key=key)
        res = search.get_cache(key=key)
        assert res is None
        res = search.get_cache(key=key, default=def_val)
        assert res == def_val
        res = search.set_cache(key=key, value=def_val, expire=10)
        assert res is True
        res = search.get_cache(key=key)
        assert res == def_val
        res = search.del_cache(key=key)
        assert res is True
        res = search.get_cache(key=key)
        assert res is None
        res = search.set_cache(key=key, value=def_val, expire=10)
        assert res is True
        res = search.get_cache(key=key)
        assert res == def_val
        search.clear_cache()
        res = search.get_cache(key=key)
        assert res is None

    def test_consul_search(self, config):
        search = ConsulKvSearch(**config)
        key = 'test/test1'
        val = 'haha'
        search.delete(key=key)
        res = search.get_key(key=key)
        assert res is None
        res = search.get(key=key)
        assert res is None
        res = search.put(key=key, value=val)
        assert res is True
        res = search.get_key(key=key)
        assert res == [{'key': key, 'value': val}]
        res = search.get(key=key)
        assert res == [{'key': key, 'value': val}]
        res = search.delete(key=key)
        assert res is True
        res = search.get(key=key)
        assert res is None


class TestFilter:

    def test_base_filter(self):

        class OddFilter(OneFilter):
            def filter_one(self, key, value, index, **kwargs):
                return int(value) % 2 == 1

        fil = OddFilter(settings=Settings())
        test_data = [
            {'key': 'test1', 'value': 1, 'index': 0, 'assert': True},
            {'key': 'test2', 'value': 2, 'index': 1, 'assert': False},
            {'key': 'test3', 'value': 3, 'index': 2, 'assert': True},
        ]
        for t in test_data:
            res = fil.filter(**t)
            assert res is t['assert']

        class SameFilter(PairedFilter):
            def filter_pair(self, key1, value1, key2, value2, index, **kwargs):
                return value1 == value2

        fil = SameFilter(settings=Settings())
        test_data = [
            {'key1': 'test1-1', 'value1': 'a', 'key2': 'test2-1', 'value2': 'a', 'index': 0, 'assert': True},
            {'key1': 'test1-2', 'value1': 'bb', 'key2': 'test2-2', 'value2': 'b', 'index': 1, 'assert': False},
            {'key1': 'test1-3', 'value1': 'c1', 'key2': 'test2-3', 'value2': 'c2', 'index': 2, 'assert': False},
        ]
        for t in test_data:
            res = fil.filter(**t)
            assert res is t['assert']

    def test_filters(self):
        fil = SkipDirectoryFilter(settings=Settings())
        test_data = [
            {'key': 'test1', 'value': 'a', 'index': 0, 'assert': True},
            {'key': 'test2/', 'value': 'a', 'index': 1, 'assert': False},
            {'key': 'test3/test', 'value': 'a', 'index': 2, 'assert': True},
            {'key': 'test4/test4/', 'value': 'a', 'index': 3, 'assert': False},
        ]
        for t in test_data:
            res = fil.filter(**t)
            assert res is t['assert']
        conf = {
            'search': {
                'regex': False,
                'fields': 'keys',
                'limit': 10,
                'query': 'test'
            }
        }
        fil = SearchFilter(settings=Settings(conf))
        test_data = [
            {'key': 'test/a', 'value': 'a', 'index': 0, 'assert': True},
            {'key': 'testa/b', 'value': 'b', 'index': 1, 'assert': True},
            {'key': 'teeest/c', 'value': 'c', 'index': 2, 'assert': False},
            {'key': 'ddd', 'value': 'd', 'index': 3, 'assert': False},
            {'key': 'eetestee', 'value': 'e', 'index': 3, 'assert': True},
        ]
        for t in test_data:
            res = fil.filter(**t)
            assert res is t['assert']
        conf = {
            'search': {
                'regex': False,
                'fields': 'values',
                'limit': 10,
                'query': 'test'
            }
        }
        fil = SearchFilter(settings=Settings(conf))
        test_data = [
            {'key': 'test/a', 'value': 'test', 'index': 0, 'assert': True},
            {'key': 'testa/b', 'value': 'btest', 'index': 1, 'assert': True},
            {'key': 'teeest/c', 'value': 'cccctestcc', 'index': 2, 'assert': True},
            {'key': 'ddd', 'value': 'ddd', 'index': 3, 'assert': False},
            {'key': 'eetestee', 'value': 'teeeest', 'index': 4, 'assert': False},
        ]
        for t in test_data:
            res = fil.filter(**t)
            assert res is t['assert']
        conf = {
            'search': {
                'regex': True,
                'fields': 'keys',
                'limit': 10,
                'query': r'^test\d+$'
            }
        }
        fil = SearchFilter(settings=Settings(conf))
        test_data = [
            {'key': 'test1', 'value': 'test', 'index': 0, 'assert': True},
            {'key': 'test/b', 'value': 'btest', 'index': 1, 'assert': False},
            {'key': 'teeest/c', 'value': 'cccctestcc', 'index': 2, 'assert': False},
            {'key': 'dddtest1', 'value': 'ddd', 'index': 3, 'assert': False},
            {'key': 'test222ee', 'value': 'teeeest', 'index': 4, 'assert': False},
            {'key': 'test3333', 'value': 'teeeest', 'index': 5, 'assert': True},
            {'key': 'test44/f', 'value': 'teeeest', 'index': 6, 'assert': False},
        ]
        for t in test_data:
            res = fil.filter(**t)
            assert res is t['assert']
        fil = DiffFilter(settings=Settings())
        test_data = [
            {'key1': 'test1-1', 'value1': 'a', 'key2': 'test2-1', 'value2': 'a', 'index': 0, 'assert': False},
            {'key1': 'test1-2', 'value1': 'bb', 'key2': 'test2-2', 'value2': 'b', 'index': 1, 'assert': True},
            {'key1': 'test1-3', 'value1': 'c1', 'key2': 'test2-3', 'value2': 'c2', 'index': 2, 'assert': True},
        ]
        for t in test_data:
            res = fil.filter(**t)
            assert res is t['assert']
