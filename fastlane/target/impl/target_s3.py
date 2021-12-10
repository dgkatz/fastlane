import logging
import io
import datetime
import time
import uuid

from fastlane.target.impl.target_athena import TargetAthena
from fastlane.target import TargetConfigSchema
from fastlane.utils import classproperty

from marshmallow import fields
import pandas as pd
import boto3

LOGGER = logging.getLogger(__name__)


class TargetS3(TargetAthena):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.s3_client = boto3.client('s3')
        self.table_schema = self.get_table_schema()
        path_parts = self.get_table_location().replace("s3://", "").split("/")
        self.s3_bucket = path_parts.pop(0)
        self.s3_table_path = "/".join(path_parts).strip('/')

    def load(self, df: pd.DataFrame):
        if self.table_version:
            df['_table_version'] = self.table_version
        buffer = io.BytesIO()
        df.to_parquet(buffer, schema=self.table_schema)
        buffer.seek(0)
        date = datetime.datetime.now().strftime('%Y/%m/%d')
        timestamp = int(time.time() * 1000000)
        object_unique_id = uuid.uuid4()
        object_name = f'{self.s3_table_path}/{date}/{timestamp}-{object_unique_id}.parquet'
        self.s3_client.upload_fileobj(
            Fileobj=buffer,
            Bucket=self.s3_bucket,
            Key=object_name
        )
        if not buffer.closed:
            buffer.truncate()
            buffer.close()
        del buffer

    @classproperty
    def target_type(self):
        return 's3'

    @classmethod
    def target_id(cls, configuration: dict):
        target_configuration = configuration['target'][cls.target_type]
        return f"{target_configuration['database']}.{target_configuration['table']}"

    @classmethod
    def configuration_schema(cls) -> TargetConfigSchema:
        return TargetS3ConfigSchema()


class TargetS3ConfigSchema(TargetConfigSchema):
    database = fields.String(required=True)
    table = fields.String(required=True)
    view_database = fields.String(required=True)
    crawler = fields.String(required=False)
    pk = fields.String(required=False)
    work_group = fields.String(required=True)
