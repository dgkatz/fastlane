from math import ceil
from io import BytesIO
import json

from fastlane.core.queue import AbstractQueue, QueueEmpty

import pandas as pd
import boto3

# TODO: queue configuration


class SQSQueue(AbstractQueue):
    def __init__(self):
        sqs = boto3.resource('sqs')
        self._sqs_queue = sqs.Queue('url')
        self._s3_client = boto3.client('s3')
        super().__init__()

    def get(self, block: bool = True, timeout: float = 0.02):
        messages = self._sqs_queue.receive_messages(
            MaxNumberOfMessages=int(ceil(timeout)),
            WaitTimeSeconds=int(timeout)
        )

        if len(messages) == 0:
            raise QueueEmpty()

        message = messages[0]
        message_body = json.loads(message.body)
        s3_bucket = message_body['s3_bucket']
        s3_object_key = message_body['s3_object_key']
        with BytesIO() as buffer:
            self._s3_client.download_fileobj(
                Bucket=s3_bucket,
                Key=s3_object_key,
                Fileobj=buffer
            )
            df = pd.read_parquet(buffer)
        message.delete()
        return df

    def put(self, df: pd.DataFrame, block: bool = True, timeout: float = None):
        with BytesIO() as buffer:
            df.to_parquet(buffer)
            self._s3_client.upload_fileobj(
                Bucket='foo',
                Key='bar.parquet',
                Fileobj=buffer
            )

        message_body = {
            's3_bucket': 'foo',
            's3_object_key': 'bar.parquet'
        }
        self._sqs_queue.send_message(MessageBody=json.dumps(message_body))

    def clear(self):
        self._sqs_queue.purge()

    def empty(self) -> bool:
        return True
