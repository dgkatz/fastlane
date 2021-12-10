import logging

from fastlane.source import Source, SourceConfigSchema
import fastlane.utils as utils

import mysql.connector
from marshmallow import fields

LOGGER = logging.getLogger(__name__)


class SourceMySQL(Source):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        try:
            source_secret = self.configuration['secret']
            source_secret_region = self.configuration['source_region']
            connection_secret = utils.get_secret(name=source_secret, region=source_secret_region)
        except KeyError:
            LOGGER.warning(f'Secret either not provided or does not exist in ssm, searching in configuration file...')
            connection_secret = {
                'host': self.configuration['host'],
                'port': self.configuration.get('port', 3306),
                'username': self.configuration['username'],
                'password': self.configuration['password']
            }

        self.source_db = self.configuration['db']
        self.source_table = self.configuration['table']
        self.source_key = self.configuration['key']

        self.select_batch_size = self.configuration['batch_size']

        self.source_connection = self._get_connection(connection_secret=connection_secret)
        self.cursor = self.source_connection.cursor(dictionary=True)

        self.is_source_key_numeric = self._pk_is_numeric()

        self.last_offset = self.offset

    def extract(self, i: int):
        select_query_params = {
            'source_db': self.source_db,
            'source_table': self.source_table,
            'source_key': self.source_key,
            'offset': self.last_offset,
            'select_batch_size': self.select_batch_size,
            'is_numeric': self.is_source_key_numeric
        }
        select_query = self._format_select_query(params=select_query_params)
        LOGGER.info(select_query)
        self.cursor.execute(select_query)
        records = self.cursor.fetchall()
        if records:
            self.last_offset = records[-1][self.source_key]
        return records

    @utils.classproperty
    def source_type(self):
        return 'mysql'

    def finish(self):
        self.source_connection.close()
        self.cursor.close()

    @staticmethod
    def _get_connection(connection_secret: dict):
        source_host = connection_secret['host']
        source_user = connection_secret['username']
        source_password = connection_secret['password']

        LOGGER.info(f'Connecting to {source_host} as {source_user}')
        return mysql.connector.connect(
            host=source_host,
            user=source_user,
            passwd=source_password,
            port=connection_secret.get('port', 3306)
        )

    @staticmethod
    def _format_select_query(params):
        source_db = params['source_db']
        source_table = params['source_table']
        source_key = params['source_key']
        offset = params['offset']
        select_batch_size = params['select_batch_size']
        is_numeric = params['is_numeric']

        formatted_offset = offset

        if not is_numeric:
            if isinstance(offset, bytearray):
                offset = offset.decode('utf-8')
            formatted_offset = f"'{offset}'"

        query = f'''
                SELECT *
                FROM {source_db}.`{source_table}`
                WHERE `{source_key}` > {formatted_offset}
                ORDER BY `{source_key}`
                LIMIT {select_batch_size}
                '''

        return query

    def _pk_is_numeric(self):
        query = f'''
                SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE table_name = '{self.source_table}' AND COLUMN_NAME = '{self.source_key}';
                '''
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            column_type = result[0]['DATA_TYPE']
        except mysql.connector.errors.ProgrammingError as e:
            LOGGER.exception(e)
            raise e

        for string_type in {'text', 'varchar', 'char'}:
            if column_type.startswith(string_type):
                return False

        return True

    @classmethod
    def configuration_schema(cls) -> SourceConfigSchema:
        return SourceMySQLConfigSchema()


class SourceMySQLConfigSchema(SourceConfigSchema):
    db = fields.String(required=True)
    table = fields.String(required=True)
    key = fields.String(required=True)
    batch_size = fields.Int(required=True, strict=True)
    host = fields.String(required=False)
    port = fields.Int(required=False)
    username = fields.String(required=False)
    password = fields.String(required=False)
