import logging
import os
from logstash.rabbit_handler import LogStashRabbitHandlerWrapper

__logstash_env = "log.example.com"

rootLogger = logging.getLogger('')
handler = LogStashRabbitHandlerWrapper('amqp://guest:guest@' + __logstash_env + '/')
rootLogger.addHandler(handler)
rootLogger.info('Initialising Logstash for environment %s', __logstash_env)
