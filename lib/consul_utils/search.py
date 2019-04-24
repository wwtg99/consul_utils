import logging
import base64
from hsettings import Settings
import consul
from diskcache import Cache


def get_consul_client(**kwargs):
    return consul.Consul(**kwargs)


class ConsulKvSearch:

    def __init__(self, settings: Settings):
        self._settings = settings
        self._cache_enabled = bool(settings.get('cache.cache_enabled', True))
        self._cache_file = settings.get('cache.cache_dir', '.consul_cache')
        self.cache_ttl = settings.get('cache.cache_ttl', 600)
        self._client = get_consul_client(**settings.get('consul', {}))
        self._root = settings.get('default_root', '')

    def get_cache(self, key, default=None, expire_time=False):
        with Cache(self._cache_file) as ref:
            return ref.get(key=self._get_cache_key(key), default=default, expire_time=expire_time)

    def set_cache(self, key, value, expire):
        with Cache(self._cache_file) as ref:
            ref.set(key=self._get_cache_key(key), value=value, expire=expire)

    def clear_cache(self):
        with Cache(self._cache_file) as ref:
            ref.clear()

    def get_key(self, key, recurse=True, raw=False, keys=False, **kwargs):
        """
        Get key value from consul kv.

        :param key:
        :param recurse:
        :param raw:
        :param keys:
        :param kwargs:
        :return:
        """
        index, vals = self._client.kv.get(key=key, recurse=recurse, keys=keys, **kwargs)
        if not raw and not keys and vals:
            res = []
            for val in vals:
                try:
                    v = val['Value'].decode('utf8') if val['Value'] is not None else None
                except Exception as e:
                    v = val['Value']
                res.append({'key': val['Key'], 'value': v})
            return res
        return vals

    def get(self, key, **kwargs):
        if not key:
            key = ''
        if self._cache_enabled:
            vals = self.get_cache(key=key)
            if vals:
                logging.info('Hit {} from cache'.format(key))
                return vals
        logging.info('Do not hit cache for {} or cache disabled'.format(key))
        vals = self.get_key(key=key, **kwargs)
        self.set_cache(key=key, value=vals, expire=self.cache_ttl)
        return vals

    def _get_cache_key(self, field) -> str:
        return base64.b64encode(':'.join([
            str(self.settings.get('consul.host', '')),
            str(self.settings.get('consul.port', 80)),
            str(self._root),
            str(field)
        ]).encode('utf-8'))

    @property
    def settings(self) -> Settings:
        return self._settings
