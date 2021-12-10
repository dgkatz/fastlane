import logging

from fastlane.target import Target, TargetConfigSchema
import fastlane.utils as utils

from marshmallow import fields
from sqlalchemy import create_engine
import pandas as pd

LOGGER = logging.getLogger(__name__)


class TargetMySQL(Target):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.primary_key = self.configuration['key']
        self.target_db = self.configuration['db']

        try:
            target_secret = self.configuration['secret']
            target_secret_region = self.configuration['secret_region']
            connection_secret = utils.get_secret(name=target_secret, region=target_secret_region)
            host = connection_secret['host']
            port = connection_secret.get('port', 3306)
            username = connection_secret['username']
            password = connection_secret['password']
        except KeyError:
            LOGGER.warning(f'Secret either not provided or does not exist in ssm, searching in configuration file...')
            host = self.configuration['host']
            port = self.configuration.get('port', 3306)
            username = self.configuration['username']
            password = self.configuration['password']

        LOGGER.info(f'Connecting to {host} as {username}')
        self.sql_engine = create_engine(
            f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{self.target_db}",
            pool_size=self.concurrency,
            max_overflow=0
        )

        if self.replication_mode == 'FULL_TABLE':
            self.drop_table()

    def drop_table(self):
        with self.sql_engine.connect() as conn:
            try:
                conn.execute(f'TRUNCATE TABLE {self.target_db}.{self.target_table}')
            except Exception as exc:
                if f"Table '{self.target_db}.{self.target_table}' doesn't exist" in str(exc):
                    return
                raise exc

    def load(self, df: pd.DataFrame):
        with self.sql_engine.connect() as conn:
            df.to_sql(
                name=self.target_table,
                schema=self.target_db,
                con=conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=self.desired_batch_size
            )

    def finish(self):
        self.sql_engine.dispose()

    @utils.classproperty
    def target_type(self):
        return 'mysql'

    def get_offset(self):
        query = f'''
            SELECT MAX({self.primary_key})
            FROM {self.target_db}.{self.target_table}
            '''
        LOGGER.info(query)

        with self.sql_engine.connect() as conn:
            result = conn.execute(query)
            offset = result[0] or -1

        LOGGER.info(f'offset of target: {offset}')

        return offset

    @classmethod
    def configuration_schema(cls) -> TargetConfigSchema:
        return TargetMySQLConfigSchema()

    @classmethod
    def target_id(cls, configuration: dict):
        target_configuration = configuration['target'][cls.target_type]
        return f"{target_configuration['db']}.{target_configuration['table']}"


class TargetMySQLConfigSchema(TargetConfigSchema):
    db = fields.String(required=True)
    table = fields.String(required=True)
    key = fields.String(required=True)
    host = fields.String(required=False)
    port = fields.Int(required=False)
    username = fields.String(required=False)
    password = fields.String(required=False)
