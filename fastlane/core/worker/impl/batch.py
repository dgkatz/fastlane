import time

from fastlane.core.worker import AbstractWorker

import boto3


class BatchWorker(AbstractWorker):
    def __init__(self, *args, **kwargs):
        self._batch_client = boto3.client('batch')

    def start(self):
        self._batch_client.submit_job()

    def stop(self, wait=False):
        self._batch_client.terminate_job()
        if wait:
            while self._batch_client.get_job()['Status'] == 'Running':
                time.sleep(1)

    def is_running(self):
        return self._batch_client.get_job()['Status'] == 'Running'
