from logging import Handler
from shared.services import get_google_cloud_storage_service
from time import gmtime, strftime


class GCSHandler(Handler):
    def __init__(self, gcs_bucket):
        super(GCSHandler, self).__init__()
        self.gcs_bucket = gcs_bucket
        self.data = []
        self.storage_service = get_google_cloud_storage_service()

    def emit(self, record):
        self.data.append(record)

    def flush(self):
        self.acquire()
        try:
            current = gmtime()
            day = strftime("%Y-%m-%d %H:%M:%S", current)
            hour = strftime("%H", current)
            product_gcs_patch = '%s/%s/%s' % (
                day, hour, self.gcs_bucket
            )

            get_google_cloud_storage_service().write_file(
                product_gcs_patch, '\n'.join(self.data), mode='w')

            # write to gcs
            self.data = []
        finally:
            self.release()

    def close(self):
        self.flush()
