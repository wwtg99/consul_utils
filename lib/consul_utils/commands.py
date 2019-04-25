import os
import logging
from itertools import product
import yaml
from colorama import Fore, Back, Style
from hsettings import Settings
from hsettings.loaders import DictLoader, YamlLoader
from .search import ConsulKvSearch
from .filters import BaseFilter, PairedFilter, SkipDirectoryFilter, SearchFilter, DiffFilter
from .reporter import OUT_ALL_KEY, OUT_FILTERED_KEY, OUT_NON_FILTERED_KEY, OUT_FLAG_KEY, TextReporter, JsonReporter, CsvReport
from .exceptions import ConsulException, FilterStop


class BaseConsulCommand:
    """
    Base command class.
    """

    filter_class = None
    filter = None
    default_config = {
        'consul': {
            'host': '',
            'port': 8500,
            'scheme': 'http',
            'token': '',
        },
        'default_root': '',
        'cache': {
            'cache_enabled': True,
            'cache_dir': '.consul_cache',
            'cache_ttl': 600
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
        },
        'search': {
            'limit': 10,
            'fields': 'key',
            'regex': False
        }
    }

    def __init__(self, settings=None, ctx=None, args=None):
        self._settings = settings or Settings(self.default_config)
        self._ctx = ctx
        self.args = args
        self._console_handlers = []
        self.parse_config()
        self._init_logger()

    def parse_config(self):
        """
        Parse and generate settings from config_file and args.
        """
        if 'config_file' in self.args and self.args['config_file']:
            if isinstance(self.args['config_file'], str) and os.path.exists(self.args['config_file']):
                d = YamlLoader.load(self.args['config_file'])
            else:
                d = yaml.load(self.args['config_file'])
            if not isinstance(d, (dict, Settings)):
                raise ConsulException('Invalid config file {}'.format(self.args['config_file']))
            self._settings.merge(d)
        cm = self.get_config_mapping()
        if cm:
            d = DictLoader.load(
                {k: v for k, v in self.args.items() if v is not None},
                key_mappings=cm,
                only_key_mappings_includes=True
            )
            self._settings.merge(d)
            if self._ctx:
                self._ctx.obj['setting'] = self._settings

    def run(self):
        """
        Run command.
        """
        consul = ConsulKvSearch(self.settings)
        # clear cache if specified
        if 'clear_cache' in self.args and self.args['clear_cache']:
            logging.info('Clear all cache')
            consul.clear_cache()
        root = self.settings.get('default_root', '')
        # get consul kv
        vals = consul.get(root)
        filtered = []
        no_filtered = []
        flags = {}
        # init filter
        if self.filter is None and self.filter_class:
            self.filter = self.filter_class(self.settings)
        if isinstance(self.filter, BaseFilter):
            try:
                for i in range(len(vals)):
                    val = vals[i]
                    # pass filter
                    if self.filter.filter(key=val['key'], value=val['value'], index=i):
                        filtered.append(val)
                    else:
                        no_filtered.append(val)
            except FilterStop as e:
                logging.debug(e)
            # get other filter results
            res = self.filter.get_results()
            if res:
                flags[self.filter.flag] = res
        else:
            logging.warning('Invalid filter {}'.format(self.filter))
            return self.parse_output({})
        # build and parse data to reporter
        data = {OUT_ALL_KEY: vals, OUT_FILTERED_KEY: filtered, OUT_NON_FILTERED_KEY: no_filtered, OUT_FLAG_KEY: flags}
        return self.parse_output(data)

    def run_and_report(self):
        """
        Run command and report.
        """
        res = self.run()
        out_type = self.settings.get('reporter.output_type', 'text')
        reporter = self.get_reporter(out_type)
        return reporter.report(res)

    def parse_output(self, data):
        """
        Parse output data.

        :param data:
        :return: data to report
        """
        return data

    def get_reporter(self, rtype):
        """
        Get reporter by output_type.

        :param rtype:
        :return: Reporter
        """
        types = {
            'text': TextReporter,
            'json': JsonReporter,
            'csv': CsvReport,
        }
        if rtype in types:
            return types[rtype](self.settings)
        raise ConsulException('Invalid output type {}'.format(rtype))

    def get_config_mapping(self):
        """
        Get args config mapping.

        :return: mapping dict
        """
        return {
            'host': 'consul.host',
            'port': 'consul.port',
            'scheme': 'consul.scheme',
            'token': 'consul.token',
            'log_level': 'log.log_level',
            'root': 'default_root',
            'output_type': 'reporter.output_type',
            'output_file': 'reporter.output_file',
        }

    def _init_logger(self):
        log_level = self._settings.get('log.log_level', 'INFO')
        log_formatter = self._settings.get('log.log_formatter')
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        logger_levels = [
            logging.ERROR,
            logging.WARNING,
            logging.INFO,
            logging.DEBUG
        ]
        for level in logger_levels:
            handler = self._create_console_level_handler(level, log_formatter)
            if handler:
                self._console_handlers.append(handler)
                root_logger.addHandler(handler)

    def _create_console_level_handler(self, level, formatter):
        level_map = {
            logging.ERROR: {
                'filter': lambda record: record.levelno >= logging.ERROR,
                'formatter': Fore.RED + formatter + Style.RESET_ALL
            },
            logging.WARNING: {
                'filter': lambda record: record.levelno == logging.WARN,
                'formatter': Fore.YELLOW + formatter + Style.RESET_ALL
            },
            logging.INFO: {
                'filter': lambda record: record.levelno == logging.INFO,
                'formatter': Fore.GREEN + formatter + Style.RESET_ALL
            },
            logging.DEBUG: {
                'filter': lambda record: record.levelno < logging.INFO,
                'formatter': Style.RESET_ALL + formatter
            }
        }
        if level in level_map:
            handler = logging.StreamHandler()
            hfilter = logging.Filter()
            hfilter.filter = level_map[level]['filter']
            handler.addFilter(hfilter)
            handler.setFormatter(logging.Formatter(level_map[level]['formatter']))
            handler.setLevel(logging.DEBUG)
            return handler
        return None

    @property
    def settings(self):
        return self._settings

    @property
    def context(self):
        return self._ctx

    def __repr__(self):
        return self.__class__.__name__


class PairedConsulCommand(BaseConsulCommand):
    """
    Base command for paired data.
    """

    def run(self):
        """
        Run command.
        """
        # get two consul settings
        settings1 = self.get_consul_conf(1)
        settings2 = self.get_consul_conf(2)
        consul1 = ConsulKvSearch(settings1)
        consul2 = ConsulKvSearch(settings2)
        # clear cache if specified
        if 'clear_cache' in self.args and self.args['clear_cache']:
            logging.info('Clear all cache')
            consul1.clear_cache()
        if 'clear_cache' in self.args and self.args['clear_cache']:
            logging.info('Clear all cache')
            consul2.clear_cache()
        root1 = settings1.get('default_root', '')
        root2 = settings2.get('default_root', '')
        # get consul kv
        vals1 = consul1.get(root1)
        vals2 = consul2.get(root2)
        vals = []
        filtered = []
        no_filtered = []
        flags = {}
        # init filter
        if self.filter is None and self.filter_class:
            self.filter = self.filter_class(self.settings)
        # add data that only exists in one side
        dt1 = {kv['key'][len(root1):]: kv['value'] for kv in vals1}
        dt2 = {kv['key'][len(root2):]: kv['value'] for kv in vals2}
        for k, v in dt1.items():
            if k not in dt2:
                vals.append(({'key': k, 'value': v}, {'key': None, 'value': None}))
                filtered.append(({'key': k, 'value': v}, {'key': None, 'value': None}))
        for k, v in dt2.items():
            if k not in dt1:
                vals.append(({'key': None, 'value': None}, {'key': k, 'value': v}))
                filtered.append(({'key': None, 'value': None}, {'key': k, 'value': v}))
        # filter data that exists in both sides
        if isinstance(self.filter, PairedFilter):
            for kv1, kv2 in product(vals1, vals2):
                k1 = kv1['key'][len(root1):]
                k2 = kv2['key'][len(root2):]
                if k1 != k2:
                    continue
                vals.append((kv1, kv2))
                # pass filter
                if self.filter.filter(key1=kv1['key'], value1=kv1['value'], key2=kv2['key'], value2=kv2['value'], index=(len(vals) - 1)):
                    filtered.append((kv1, kv2))
                else:
                    no_filtered.append((kv1, kv2))
            res = self.filter.get_results()
            if res:
                flags[self.filter.flag] = res
        else:
            logging.warning('Invalid filter {}'.format(self.filter))
            return self.parse_output({})
        # build and parse data to reporter
        data = {OUT_ALL_KEY: vals, OUT_FILTERED_KEY: filtered, OUT_NON_FILTERED_KEY: no_filtered, OUT_FLAG_KEY: flags}
        return self.parse_output(data)

    def get_consul_conf(self, n):
        settings = self.settings.clone()
        n = str(n)
        if 'host' + n in self.args and self.args['host' + n]:
            host = self.args['host' + n]
            settings.set('consul.host', host)
        if 'port' + n in self.args and self.args['port' + n]:
            port = self.args['port' + n]
            settings.set('consul.port', port)
        if 'scheme' + n in self.args and self.args['scheme' + n]:
            scheme = self.args['scheme' + n]
            settings.set('consul.scheme', scheme)
        if 'token' + n in self.args and self.args['token' + n]:
            token = self.args['token' + n]
            settings.set('consul.token', token)
        if 'root' + n in self.args and self.args['root' + n]:
            root = self.args['root' + n]
            settings.set('default_root', root)
        return settings


class DumpCommand(BaseConsulCommand):

    filter_class = SkipDirectoryFilter


class SearchCommand(BaseConsulCommand):

    filter_class = SearchFilter

    def get_config_mapping(self):
        m = super().get_config_mapping()
        m.update({
            'regex': 'search.regex',
            'fields': 'search.fields',
            'limit': 'search.limit',
            'query': 'search.query',
        })
        return m


class DiffCommand(PairedConsulCommand):

    filter_class = DiffFilter

    def __init__(self, settings=None, ctx=None, args=None):
        super().__init__(settings, ctx, args)
        if 'with_same' in args and args['with_same']:
            self.settings.set('reporter.show_no_filtered', True)
