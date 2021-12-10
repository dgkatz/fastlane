import logging
from datetime import datetime

from fastlane.target import Target, TargetConfigSchema
import fastlane.utils as utils

from marshmallow import fields
import influxdb
import pandas as pd

LOGGER = logging.getLogger(__name__)


class TargetInfluxDB(Target):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.database = self.configuration[self.target_type]['database']
        self.measurement = self.configuration[self.target_type]['measurement']
        self.tag_columns = self.configuration[self.target_type]['tag_columns']
        self.field_columns = self.configuration[self.target_type]['field_columns']

        self.target_secret = self.configuration[self.target_type]['secret']
        self.target_secret_region = self.configuration['ssm']['region']
        self.connection_secret = utils.get_secret(name=self.target_secret, region=self.target_secret_region)

        self.influx_host = self.connection_secret['host']
        self.influx_port = self.connection_secret['port']
        self.influx_user = self.connection_secret['user']
        self.influx_password = self.connection_secret['password']

        self.influx_client = influxdb.DataFrameClient(
            host=self.influx_host,
            port=self.influx_port,
            username=self.influx_user,
            password=self.influx_password
        )

    def load(self, df: pd.DataFrame):
        df.insert(0, "current_time", datetime.now())
        df.set_index('current_time', inplace=True)
        self.influx_client.write_points(
            dataframe=df,
            measurement=self.measurement,
            tag_columns=self.tag_columns,
            field_columns=self.field_columns,
            time_precision='ms',
            database=self.database
        )

    def finish(self):
        self.influx_client.close()

    @utils.classproperty
    def target_type(self):
        return 'influxdb'

    def get_offset(self):
        return -1

    @classmethod
    def target_id(cls, configuration: dict):
        target_configuration = configuration['target'][cls.target_type]
        return f"{target_configuration['database']}.{target_configuration['measurement']}"

    @classmethod
    def configuration_schema(cls) -> TargetConfigSchema:
        return TargetInfluxDBConfigSchema()


class TargetInfluxDBConfigSchema(TargetConfigSchema):
    database = fields.String(required=True)
    measurement = fields.String(required=True)
    tag_columns = fields.List(required=False, cls_or_instance=fields.String(required=True))
    field_columns = fields.List(required=False, cls_or_instance=fields.String(required=True))
