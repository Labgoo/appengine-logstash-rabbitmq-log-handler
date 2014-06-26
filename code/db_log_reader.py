import os
import time
from datetime import datetime
import webapp2
import ast
from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.appengine.api import logservice

from rabbit_handler import LogStashRabbitHandler

from mapreduce import mapreduce_pipeline
from mapreduce.output_writers import OutputWriter, _get_params
from mapreduce.pipeline_base import PipelineBase


default_shards = 1


class Log2Logstash2(PipelineBase):
    def finalized(self):
        pass

    def run(self, params, start_time, end_time, modules, shards):
        output_writer_spec = "libs.logstash.db_log_reader.LogstashRabbitWriter"
        yield mapreduce_pipeline.MapperPipeline(
            "log2stash",
            "libs.logstash.db_log_reader.log2stash",
            "mapreduce.input_readers.LogInputReader",
            output_writer_spec=output_writer_spec,
            params={
                "input_reader": {
                    "start_time": start_time,
                    "end_time": end_time,
                    "include_app_logs": True,
                    "module_versions": modules
                },
                "output_writer": params,
                "root_pipeline_id": self.root_pipeline_id
            },
            shards=shards)


def logging_level(level):
    if level == logservice.LOG_LEVEL_DEBUG:
        return 'DEBUG'
    elif level == logservice.LOG_LEVEL_INFO:
        return 'INFO'
    elif level == logservice.LOG_LEVEL_WARNING:
        return 'WARNING'
    elif level == logservice.LOG_LEVEL_ERROR:
        return 'ERROR'
    elif level == logservice.LOG_LEVEL_CRITICAL:
        return 'CRITICAL'
    else:
        return 'UNKNOWN'


def add_extra_fields(message_dict, extra_fields):
    for key, value in extra_fields.items():
        if not key.startswith('_'):
            message_dict['_%s' % key] = repr(value)

    return message_dict


class LogstashRabbitWriter(OutputWriter):
    def __init__(self, app_id, host, service_name=None, level=None):
        super(LogstashRabbitWriter, self).__init__()
        self.app_id = app_id
        self.host = host
        self.service_name = service_name or app_identity.get_application_id()
        self.level = level or logservice.LOG_LEVEL_DEBUG
        self.handler = LogStashRabbitHandler(host) if host else None

    @classmethod
    def validate(cls, mapper_spec):
        pass

    @classmethod
    def init_job(cls, mapreduce_state):
        pass

    @classmethod
    def finalize_job(cls, mapreduce_state):
        pass

    @classmethod
    def from_json(cls, state):
        state = state or {}
        return cls(state.get("app_id"), state.get("host"), state.get("service_name"), level=state.get("level"))

    def finalize(self, ctx, shard_state):
        pass

    @classmethod
    def get_filenames(cls, mapreduce_state):
        pass


    def to_json(self):
        return {
            "app_id": self.app_id,
            "host": self.host,
            "service_name": self.service_name,
            "level": self.level}

    @classmethod
    def create(cls, mr_spec, shard_number, shard_attempt, _writer_state=None):
        writer_spec = _get_params(mr_spec.mapper, allow_old=False)
        return cls(
            writer_spec["app_id"],
            writer_spec["host"],
            writer_spec.get("service_name"),
            level=writer_spec.get("level"))

    def write(self, data):
        if not self.handler:
            return
        if data.get('facility', '').startswith('/mapreduce'):
            return

        app_logs = data.get("app_logs") or []

        if app_logs:
            request_data = {}
            request_data.update(data)

            del request_data["app_logs"]
        else:
            request_data = data
        request_data['app_id'] = self.app_id
        self.handler.send(self.handler.formatter.serialize(request_data))

        for app_log in app_logs:
            if app_log.level < self.level:
                continue

            if app_log.message.startswith('Saved; key: __appstats__'):
                continue

            # Messages that start with '{' are assumed to be a serialized dict.
            if app_log.message.startswith('{'):
                structured_message = ast.literal_eval(app_log.message)
            else:
                structured_message = {'message': app_log.message}

            app_log_data = dict({
                "app_id": self.app_id,
                "host": app_identity.get_application_id(),
                "log_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(app_log.time)),
                "level": logging_level(app_log.level),
                "facility": data.get("facility"),
                "message": app_log.message,
                "request_id": data.get("request_id")}, **structured_message)
            self.handler.send(self.handler.formatter.serialize(app_log_data))


def log2stash(l):
    def level_from_status(status):
        return logservice.LOG_LEVEL_INFO if status < 400 else logservice.LOG_LEVEL_ERROR

    yield {
        "log_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(l.start_time)),
        "level": logging_level(level_from_status(l.status)),
        "facility": l.resource,
        "message": "%s %s" % (l.method, l.resource),
        "app_logs": l.app_logs,
        "request_id": l.request_id,
        "method": l.method,
        "latency": l.latency,
        "cost": l.cost,
        "status": l.status,
        "host": l.host,
        "module": l.module_id,
        "user_agent": l.user_agent if l.user_agent else None,
        'version_id': os.environ['CURRENT_VERSION_ID'],
    }


class LoggingConfig(ndb.Model):
    last_logging_time = ndb.model.FloatProperty(required=True)
    last_logging_timestamp = ndb.model.ComputedProperty(lambda self: datetime.fromtimestamp(self.last_logging_time))


max_logging_time = 3600 * 24  # dump dump more than 24 hours


class LogUploadHandler(webapp2.RequestHandler):
    @classmethod
    def get_module_versions(cls, version_id):
        raise NotImplementedError("get_module_versions() not implemented in %s" % cls)

    @classmethod
    def get_logstash_host(cls):
        raise NotImplementedError("get_logstash_host() not implemented in %s" % cls)

    @classmethod
    def get_app_id(cls):
        raise NotImplementedError("get_logstash_host() not implemented in %s" % cls)

    def get(self):
        def get_int(name, default_value):
            value = self.request.get(name)

            if not value:
                try:
                    value = int(value)
                except ValueError:
                    value = default_value
            else:
                value = default_value

            return value

        now = time.time()

        logging_config = LoggingConfig.get_by_id("main")

        if logging_config is None:
            end_time = now - 60 * get_int('minutes', 1)
            logging_config = LoggingConfig(id="main", last_logging_time=now)
        else:
            end_time = logging_config.last_logging_time
            logging_config.last_logging_time = now

        if now - end_time > max_logging_time:
            end_time = now - max_logging_time

        version = os.environ["CURRENT_VERSION_ID"].split(".")[0]
        shards = get_int('shards', default_shards)

        params = {
            "app_id": self.get_app_id(),
            "level": logservice.LOG_LEVEL_DEBUG,
            "host": 'amqp://guest:guest@%s/' % self.get_logstash_host()}

        versions = self.get_module_versions(version)
        p = Log2Logstash2(params, end_time, now, versions, shards)
        p.start()

        logging_config.put()

