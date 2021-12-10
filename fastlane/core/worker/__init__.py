from fastlane.core.worker import *
from fastlane.core.worker.impl.thread import *
from fastlane.core.worker.impl.process import *
from fastlane.core.worker.impl.batch import *


def get_worker_type(worker_type: str):
    if worker_type == 'thread':
        return ThreadWorker
    elif worker_type == 'process':
        return ProcessWorker
    elif worker_type == 'batch':
        return BatchWorker
    else:
        raise NotImplementedError()
