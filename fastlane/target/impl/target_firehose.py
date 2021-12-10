import logging
import json

from fastlane.target.impl.target_athena import TargetAthena
from fastlane.target import TargetConfigSchema
from fastlane.utils import rate_limited, classproperty

import boto3
import pandas as pd
from marshmallow import Schema, fields

LOGGER = logging.getLogger(__name__)


class TargetFirehose(TargetAthena):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.firehose_client = boto3.client('firehose')
        self.delivery_stream = self.configuration['delivery_stream']

    @rate_limited(max_per_second=2000)
    def load(self, df: pd.DataFrame):
        if self.table_version:
            df['_table_version'] = self.table_version
        records = df.to_dict('records')
        kinesis_records = []
        for record in records:
            kinesis_records.append({'Data': json.dumps(record, default=str).encode('utf_8')})
        self.firehose_client.put_record_batch(
            DeliveryStreamName=self.delivery_stream,
            Records=kinesis_records
        )

    @classproperty
    def target_type(self):
        return 'firehose'

    @classmethod
    def target_id(cls, configuration: dict):
        target_configuration = configuration['target'][cls.target_type]
        return f"{target_configuration['database']}.{target_configuration['table']}"

    @classmethod
    def configuration_schema(cls) -> Schema:
        return TargetFirehoseConfigSchema()


class TargetFirehoseConfigSchema(TargetConfigSchema):
    database = fields.String(required=True)
    table = fields.String(required=True)
    view_database = fields.String(required=True)
    crawler = fields.String(required=False)
    pk = fields.String(required=False)
    work_group = fields.String(required=True)
    delivery_stream = fields.String(required=True)
