from fastlane.core.queue import *
from fastlane.core.queue.impl.thread import *
from fastlane.core.queue.impl.process import *
from fastlane.core.queue.impl.sqs import *


def get_queue_type(queue_type: str):
    if queue_type == 'thread':
        return ThreadQueue
    elif queue_type == 'process':
        return ProcessQueue
    elif queue_type == 'sqs':
        return SQSQueue
    else:
        raise NotImplementedError()
