
from logging import Handler


class GCSHandler(Handler):

    def __init__(self, gcs_bucket):
        super(GCSHandler, self).__init__()
        self.data = []

        return (len(self.buffer) >= self.capacity)

    def emit(self, record):
        self.data.append(record)

    def flush(self):
        self.acquire()
        try:
            # write to gcs
            self.data = []
        finally:
            self.release()

    def close(self):
        self.flush()
