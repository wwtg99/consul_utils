import logging
import base64
import consul
from diskcache import Cache


class ConsulKvSearch:
    """
    Search in the consul key value.
    """

    def __init__(self, host, port, scheme, token, verify=True, cert=None, root='', cache_enabled=True,
                 cache_dir='.consul_cache', cache_ttl=600):
        self._host = host
        self._port = port
        self._scheme = scheme
        self._token = token
        self._verify = verify
        self._cert = cert
        self._cache_enabled = cache_enabled
        self._cache_dir = cache_dir
        self.cache_ttl = cache_ttl
        self._root = root
        self._client = consul.Consul(host=host, port=port, token=token, scheme=scheme, verify=verify, cert=cert)

    def get_cache(self, key, default=None, expire_time=False):
        if self._cache_enabled:
            return self.cache.get(key=self._get_cache_key(key), default=default, expire_time=expire_time)
        return None

    def set_cache(self, key, value, expire):
        if self._cache_enabled:
            return self.cache.set(key=self._get_cache_key(key), value=value, expire=expire)
        return False

    def del_cache(self, key):
        if self._cache_enabled:
            return self.cache.delete(key=self._get_cache_key(key))
        return True

    def clear_cache(self):
        if self._cache_enabled:
            self.cache.clear()

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
        """
        Get key from cache, if not hit in the cache, then find in the consul.

        :param key:
        :return:
        """
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

    def put(self, key, value, **kwargs):
        """
        Put key value in consul and cache if enabled.

        :param key:
        :param value:
        :param kwargs:
        :return:
        """
        res = self._client.kv.put(key=key, value=value, **kwargs)
        return res

    def delete(self, key, recurse=None, **kwargs):
        res = self._client.kv.delete(key=key, recurse=recurse, **kwargs)
        if self._cache_enabled:
            self.del_cache(key=key)
        return res

    def _get_cache_key(self, field) -> str:
        return base64.b64encode(':'.join([
            str(self._host),
            str(self._port),
            str(self._root),
            str(field)
        ]).encode('utf-8'))

    @property
    def cache(self) -> Cache:
        with Cache(self._cache_dir) as ref:
            return ref
