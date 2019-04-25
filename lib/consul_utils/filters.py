import re
from .exceptions import FilterStop


class BaseFilter:
    """
    Base filter class.
    """

    filter_flag = 'default'

    def __init__(self, settings, flag='default'):
        self._settings = settings
        self.flag = flag or self.filter_flag
        self._params = {}
        self.results = None

    def filter(self, **kwargs) -> bool:
        """
        Filter data, return true if data passed filter.
        """
        return False

    def get_results(self):
        """
        Get other filter results.

        :return: results
        """
        return self.results

    @property
    def settings(self):
        return self._settings

    def __repr__(self):
        return self.__class__.__name__


class OneFilter(BaseFilter):
    """
    Base filter for one data.
    """

    def filter_one(self, key, value, index, **kwargs) -> bool:
        return False

    def filter(self, **kwargs) -> bool:
        return self.filter_one(**kwargs)


class PairedFilter(BaseFilter):
    """
    Base filter for paired data.
    """

    def filter_pair(self, key1, value1, key2, value2, index, **kwargs) -> bool:
        return False

    def filter(self, **kwargs) -> bool:
        return self.filter_pair(**kwargs)


class NoFilter(OneFilter):
    """
    All data passed filter.
    """

    def filter_one(self, key, value, index, **kwargs):
        return True


class SkipDirectoryFilter(OneFilter):
    """
    Filter all directory keys.
    """

    def filter_one(self, key, value, index, **kwargs):
        if key.endswith('/'):
            return False
        return True


class SearchFilter(OneFilter):
    """
    Search filter.
    """

    def __init__(self, settings, flag='default'):
        super().__init__(settings, flag)
        self.regex = bool(settings.get('search.regex', False))
        self.fields = settings.get('search.fields', 'keys')
        self.limit = int(settings.get('search.limit', 10))
        self.compiled_pattern = None
        self.num = 0

    def filter_one(self, key, value, index, **kwargs):
        query = self.get_query()
        if self.fields == 'keys':
            data = key
        else:
            data = value
            if data is None:
                return False
        if self.regex:
            s = query.search(data)
            res = True if s else False
        else:
            res = query in data
        if res:
            self.num += 1
            if self.num > self.limit:
                raise FilterStop('Search hit reach limit {}'.format(self.limit))
        return res

    def get_query(self):
        query = self.compiled_pattern
        if not query:
            # compile for the first time
            query = self.settings.get('search.query')
            if not query:
                raise ValueError('No query specified')
            if self.regex:
                query = re.compile(query)
            self.compiled_pattern = query
        return query


class DiffFilter(PairedFilter):
    """
    Diff filter.
    """

    def filter_pair(self, key1, value1, key2, value2, index, **kwargs) -> bool:
        return value1 != value2
