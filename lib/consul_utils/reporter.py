

OUT_ALL_KEY = 'scan'
OUT_FILTERED_KEY = 'filtered'
OUT_NON_FILTERED_KEY = 'non_filtered'
OUT_FLAG_KEY = 'flags'


class ReporterStream:
    """
    Base class for report stream.
    """

    def open(self):
        """
        Open stream.
        """
        return self

    def append(self, data):
        """
        Append data to stream.

        :param data:
        """
        pass

    def close(self):
        """
        Close stream.
        """
        pass

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ConsoleStream(ReporterStream):
    """
    Stream to console.
    """

    def append(self, data):
        print(data)


class FileStream(ReporterStream):
    """
    Stream to file.
    """

    def __init__(self, filepath, mode='w'):
        self._filepath = filepath
        self._mode = mode
        self._fp = None

    def open(self):
        self._fp = open(self._filepath, self._mode)
        return self

    def append(self, data):
        self._fp.write(data + '\n')

    def close(self):
        self._fp.close()


class BaseReporter:
    """
    Base class for reporter.
    """

    def __init__(self, settings):
        self._settings = settings

    def trim_data(self, data):
        """
        Remove unused data by reporter settings.

        :param data:
        :return: trimmed data
        """
        if not self.settings.get('reporter.show_all_scan', False):
            del data[OUT_ALL_KEY]
        if not self.settings.get('reporter.show_filtered', True):
            del data[OUT_FILTERED_KEY]
        if not self.settings.get('reporter.show_no_filtered', False):
            del data[OUT_NON_FILTERED_KEY]
        if not self.settings.get('reporter.show_flags', False):
            del data[OUT_FLAG_KEY]
        return data

    def format(self, data, **kwargs):
        """
        Format data, yield each data.

        :param data:
        :return:
        """
        yield data

    def report(self, data, **kwargs):
        """
        Report data.

        :param data:
        :return:
        """
        data = self.trim_data(data)
        if self.settings.get('reporter.output_file'):
            with FileStream(self.settings.get('reporter.output_file')) as stream:
                for line in self.format(data, **kwargs):
                    stream.append(line)
        else:
            with ConsoleStream() as stream:
                for line in self.format(data, **kwargs):
                    stream.append(line)

    @property
    def settings(self):
        return self._settings


class TextReporter(BaseReporter):
    """
    Text format reporter.
    """

    def format(self, data, **kwargs):
        if OUT_ALL_KEY in data:
            yield '\nScan:'
            for d in data[OUT_ALL_KEY]:
                yield self.to_text(d)
        if OUT_NON_FILTERED_KEY in data:
            yield '\nNon Filtered:'
            for d in data[OUT_NON_FILTERED_KEY]:
                yield self.to_text(d)
        if OUT_FILTERED_KEY in data:
            yield '\nFiltered:'
            for d in data[OUT_FILTERED_KEY]:
                yield self.to_text(d)
        if OUT_FLAG_KEY in data:
            yield '\nFlags:'
            for d in data[OUT_FLAG_KEY]:
                yield self.to_text(d)

    def to_text(self, d):
        if 'key' in d and 'value' in d:
            return '{}: {}'.format(d['key'], d['value'])
        elif isinstance(d, (tuple, list)):
            return '---> {}: {}\n<--- {}: {}'.format(d[0]['key'], d[0]['value'], d[1]['key'], d[1]['value'])
        else:
            return str(d)


class JsonReporter(BaseReporter):
    """
    Json format reporter.
    """

    def format(self, data, **kwargs):
        import json
        yield json.dumps(data, indent=4)


class CsvReport(BaseReporter):
    """
    Csv format reporter.
    """

    def format(self, data, **kwargs):
        if OUT_ALL_KEY in data:
            yield '\nScan:'
            for d in data[OUT_ALL_KEY]:
                yield self.to_csv(d)
        if OUT_NON_FILTERED_KEY in data:
            yield '\nNon Filtered:'
            for d in data[OUT_NON_FILTERED_KEY]:
                yield self.to_csv(d)
        if OUT_FILTERED_KEY in data:
            yield '\nFiltered:'
            for d in data[OUT_FILTERED_KEY]:
                yield self.to_csv(d)
        if OUT_FLAG_KEY in data:
            yield '\nFlags:'
            for d in data[OUT_FLAG_KEY]:
                yield self.to_csv(d)

    def to_csv(self, d):
        if 'key' in d and 'value' in d:
            return '{},{}'.format(d['key'], d['value'])
        elif isinstance(d, (tuple, list)):
            return '{},{},{},{}'.format(d[0]['key'], d[0]['value'], d[1]['key'], d[1]['value'])
        else:
            return str(d)
