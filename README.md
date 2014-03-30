#App Engine RabbitMQ Logstash Handler


A Google App Engine log handler logging to RabbitMQ in the logstash format
##Installation on a Google App Engine app

###Preface - Recommended dependency management procedure on GAE
It is recommended that you follow a dependency management procedure in which you have a folder for third-party dependencies adjacent to your application code folder. By creating symbolic links from inside your application code folder to that adjacent folder you can better control the actual folder being bundled with your app when you upload it, making the entire dependency managmenet + python include procedure easier 
###Dependencies
amqplib 1.2.0 - bundled

###Installing the logging handler
####Symlinking into the application folder
 
The `code` folder inside this repository should be symlinked to somewhere under your app, preferably under a `libs` folder otherwise you would have to fork this repository.
####Modifying appengine_config.py
Your `appengine_config.py` file should be modified to add the logging handler to the default logger. See an example file at [appengine_config.py](/example_appengine_config.py)


##Map Reduce Job Logstash

include the map-reduce [lib](https://developers.google.com/appengine/docs/python/dataprocessing/mapreduce_library) into your codebase

Inherit from [LogUploadHandler](code/db_log_reader.py)

Example :

```python
class MallpadLogsUploadHandler(LogUploadHandler):
    def get_module_versions(self, version_id):
        return [('module1', version_id), ('module2', version_id)]

    def get_logstash_host(self):
        return 'log.yourcompany.com'

    def get_app_id(self):
        return 'application_id'
```

Create the relevant routes entry and a cron entry for that handler

```
- description: dump logs to logstash
  url: /scheduler/logs/upload
  schedule: every 1 minutes synchronized
```

##License
GPL v2. Please refer to the [license](/LICENSE) file for a complete license declaration.

