import datadog
import logging


LOG_LEVEL_ALERT_TYPE_MAPPINGS = {
    logging.DEBUG: "info",
    logging.INFO: "info",
    logging.WARNING: "warning",
    logging.ERROR: "error",
    logging.CRITICAL: "error"
}


class AbstractMessage(object):
    """
    Message to be constructed by a Formatter from a LogRecord
    Incapsulates a message for DataDog
    """
    def __init__(self, formatter, record):
        self.init(formatter, record)

    def init(self, formatter, record):
        raise NotImplementedError('init not implemented')


class EventMessage(AbstractMessage):
    def init(self, formatter, record):
        text = formatter.format(record)

        if record.mentions is not None:
            text = "\n\n".join([text, " ".join(record.mentions)])

        self.data = {
            'title': getattr(record, 'title', record.name),
            'text': text,
            'alert_type': None,
            'aggregation_key': getattr(record, 'aggregation_key', None),
            'source_type_name': getattr(record, 'source_type_name', None),
            'date_happened': getattr(record, 'date_happened', None),
            'priority': getattr(record, 'priority', None),
            'tags': getattr(record, 'tags', None),
            'hostname': getattr(record, 'hostname', None)
        }

        if record.levelno in LOG_LEVEL_ALERT_TYPE_MAPPINGS:
            self.data['alert_type'] = LOG_LEVEL_ALERT_TYPE_MAPPINGS[record.levelno]


class GaugeMessage(AbstractMessage):
    def init(self, formatter, record):
        self.metric_name = record.msg

        if getattr(record, 'args', None) and isinstance(record.args, list) and len(record.args) > 0:
            self.values = record.args
        else:
            self.values = [0]

        self.tags = getattr(record, 'tags', None)
        self.sample_rate = getattr(record, 'sample_rate', 1)


class BaseDatadogFormatter(logging.Formatter):
    def __init__(self, default_message_class, message_class_keyword, tags=None, mentions=None, *args, **kwargs):
        super(BaseDatadogFormatter, self).__init__(*args, **kwargs)
        self.tags = tags
        self.mentions = mentions
        self.default_message_class = default_message_class
        self.message_class_keyword = message_class_keyword

    def format(self, record):
        message_class = self.resolve_message(record)
        self.patch_record(record)
        return message_class(super(BaseDatadogFormatter, self), record)

    def patch_record(self, record):
        if self.mentions is not None:
            if record.mentions and isinstance(record.mentions, list):
                record.mentions += self.mentions
            elif record.mentions:
                record.mentions = [record.mentions] + self.mentions
            else:
                record.mentions = self.mentions

        if self.tags is not None:
            if record.tags and isinstance(record.tags, list):
                record.tags += self.tags
            elif record.tags:
                record.tags = [record.tags] + self.tags
            else:
                record.tags = self.tags

    def resolve_message(self, record):
        todo = getattr(record, self.message_class_keyword, None)
        if todo == 'event':
            return EventMessage
        elif todo == 'gauge':
            return GaugeMessage
        else:
            return self.default_message_class


class AbstractDatadogLoggingHandler(logging.Handler):
    def __init__(self, tags=None, mentions=None, **kwargs):
        default_message_class = kwargs.pop('default_message_class', EventMessage)
        message_class_keyword = kwargs.pop('message_class_keyword', 'datadog_todo')

        super(AbstractDatadogLoggingHandler, self).__init__(**kwargs)

        self.formatter = BaseDatadogFormatter(default_message_class, message_class_keyword, tags, mentions)

    def emit(self, record):
        try:
            msg = self.format(record)
            if isinstance(msg, AbstractMessage):
                self.push_message(msg)
            else:
                raise TypeError('formatter returns a message not implementing AbstractMessage', msg)
        except:
            self.handleError(record)

    def push_message(self, message):
        raise NotImplementedError('push_message not implemented')


class StatsdDatadogLoggingHandler(AbstractDatadogLoggingHandler):
    def push_message(self, message):
        if isinstance(message, EventMessage):
            datadog.statsd.event(**message.data)

        elif isinstance(message, GaugeMessage):
            for value in message.values:
                datadog.statsd.gauge(message.metric_name, value, message.tags, message.sample_rate)

        else:
            raise TypeError('unknown message', message)


class ApiDatadogLoggingHandler(AbstractDatadogLoggingHandler):
    def push_message(self, message):
        if isinstance(message, EventMessage):
            datadog.api.Event.create(**message.data)

        elif isinstance(message, GaugeMessage):
            datadog.api.Metric.send(metric=message.metric_name, points=message.values, tags=message.tags, sample_rate=message.sample_rate)

        else:
            raise TypeError('unknown message', message)


class ThreadStatsDatadogLoggingHandler(AbstractDatadogLoggingHandler):
    def __init__(self, *args, **kwargs):
        ts_args = {}
        start_args = {}

        for arg in ['namespace', 'constant_tags']:
            if kwargs.get(arg, None) is not None:
                ts_args[arg] = kwargs.pop(arg)
            else:
                kwargs.pop(arg, None)  # wipe it out if it is None

        for arg in ['flush_interval', 'roll_up_interval', 'device',
                    'flush_in_thread', 'flush_in_greenlet', 'disabled']:
            if kwargs.get(arg, None) is not None:
                start_args[arg] = kwargs.pop(arg)
            else:
                kwargs.pop(arg, None)  # wipe it out if it is None

        self.stats = datadog.ThreadStats(**ts_args)
        self.stats.start(**start_args)

    def __del__(self):
        if self.stats:
            self.stats.stop()

    def push_message(self, message):
        if isinstance(message, EventMessage):
            self.stats.event(**message.data)

        elif isinstance(message, GaugeMessage):
            for value in message.values:
                self.stats.gauge(message.metric_name, value, tags=message.tags, sample_rate=message.sample_rate)

        else:
            raise TypeError('unknown message', message)
