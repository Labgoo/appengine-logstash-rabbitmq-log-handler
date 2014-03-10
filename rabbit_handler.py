from logging import Filter
from logging.handlers import SocketHandler
from urlparse import urlparse
from libs.amqplib import client_0_8 as amqp
from clientstack import ClientStack

import formatter

_ifnone = lambda v, x: x if v is None else v


class LogStashRabbitHandlerWrapper(object):
    def __init__(self, url):
        self.url = url
        self.handler_stack = ClientStack()

    def __get_handler(self):
        handler = self.handler_stack.get()
        if handler is None:
            handler = LogStashRabbitHandler(self.url)
            self.handler_stack.push(handler)
        return handler

    def __getattr__(self, attr):
        orig_attr = self.__get_handler().__getattribute__(attr)
        return orig_attr


class LogStashRabbitHandler(SocketHandler):
    """RabbitMQ Extended Log Format handler

    :param url: RabbitMQ URL (ex: amqp://guest:guest@localhost:5672/).
    :param exchange: RabbitMQ exchange. Default 'logging.gelf'.
        A queue binding must be defined on the server to prevent
        log messages from being dropped.
    :param debugging_fields: Send debug fields if true (the default).
    :param extra_fields: Send extra fields on the log record to graylog
        if true (the default).
    :param fqdn: Use fully qualified domain name of localhost as source
        host (socket.getfqdn()).
    :param exchange_type: RabbitMQ exchange type (default 'fanout').
    :param localname: Use specified hostname as source host.
    :param facility: Replace facility with specified value. If specified,
        record.name will be passed as `logger` parameter.
    """

    def __init__(self, url, exchange='logstash', debugging_fields=True,
                 extra_fields=True, fqdn=False, exchange_type='fanout', localname=None,
                 facility=None):
        self.url = url
        parsed = urlparse(url)
        if parsed.scheme != 'amqp':
            raise ValueError('invalid URL scheme (expected "amqp"): %s' % url)
        host = parsed.hostname or 'localhost'
        port = _ifnone(parsed.port, 5672)
        self.cn_args = {
            'host': '%s:%s' % (host, port),
            'userid': _ifnone(parsed.username, 'guest'),
            'password': _ifnone(parsed.password, 'guest'),
            'virtual_host': '/',
            'insist': False,
        }
        self.exchange = exchange
        self.debugging_fields = debugging_fields
        self.extra_fields = extra_fields
        self.fqdn = fqdn
        self.exchange_type = exchange_type
        self.localname = localname
        self.facility = facility
        SocketHandler.__init__(self, host, port)
        self.formatter = formatter.LogstashFormatter('logstash', [], fqdn)

        self.addFilter(ExcludeFilter('amqplib'))

    def makeSocket(self, timeout=1):
        return RabbitSocket(self.cn_args, timeout, self.exchange,
                            self.exchange_type)

    def makePickle(self, record):
        return self.formatter.format(record)


class RabbitSocket(object):
    def __init__(self, cn_args, timeout, exchange, exchange_type):
        self.cn_args = cn_args
        self.timeout = timeout
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.connection = amqp.Connection(
            connection_timeout=timeout, **self.cn_args)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange,
            type=self.exchange_type,
            durable=True,
            auto_delete=False,
        )

    def sendall(self, data):
        msg = amqp.Message(data, delivery_mode=2)
        self.channel.basic_publish(msg, exchange=self.exchange)

    def close(self):
        try:
            self.connection.close()
        except Exception:
            pass


class ExcludeFilter(Filter):
    def __init__(self, name):
        """Initialize filter.

        Initialize with the name of the logger which, together with its
        children, will have its events excluded (filtered out).
        """

        if not name:
            raise ValueError('ExcludeFilter requires a non-empty name')
        self.name = name
        self.nlen = len(name)

    def filter(self, record):
        return not (record.name.startswith(self.name) and (
            len(record.name) == self.nlen or record.name[self.nlen] == "."))
