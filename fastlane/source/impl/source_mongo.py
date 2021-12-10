import logging

from fastlane.source import Source, SourceConfigSchema
import fastlane.utils as utils

from marshmallow import fields
import pymongo

LOGGER = logging.getLogger(__name__)


class SourceMongoDB(Source):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.select_batch_size = self.configuration['batch_size']
        self.filter_statement = self._gen_filter_statement()
        self.replication_key = self.configuration.get('replication_key', '_id')
        self.database_name = self.configuration['database']
        self.collection_name = self.configuration['collection']

        try:
            source_secret = self.configuration[self.source_type]['secret']
            source_secret_region = self.configuration['ssm']['region']
            connection_secret = utils.get_secret(name=source_secret, region=source_secret_region)
        except KeyError:
            LOGGER.warning(f'Secret either not provided or does not exist in ssm, searching in configuration file...')
            connection_secret = {
                "host": self.configuration['host'],
                "port": int(self.configuration['port']),
                "username": self.configuration['user'],
                "password": self.configuration['password']
            }

        self.mongo_client = pymongo.MongoClient(**connection_secret)

        self.database = self.mongo_client[self.database_name]
        self.collection = self.database[self.collection_name]

        LOGGER.info(f'Connected to MongoDB host: %s, version: %s',
                    self.mongo_client.address,
                    self.mongo_client.server_info().get('version', 'unknown'))

        self.last_offset = self.offset

    def extract(self, i: int):
        filter_statement = self.filter_statement
        filter_statement.update({self.replication_key: {'$gt': self.last_offset}})
        with self.collection.find(
                filter_statement,
                {'_id': 0},
                sort=[(self.replication_key, pymongo.ASCENDING)]).limit(self.select_batch_size) as cursor:
            data = list(cursor)
            if data:
                self.last_offset = data[-1][self.replication_key]
            return data

    def finish(self):
        self.mongo_client.close()

    @utils.classproperty
    def source_type(self):
        return 'mongodb'

    def _gen_filter_statement(self) -> dict:
        filter_fields = self.configuration.get('filter_fields', {})
        filter_statement = {}
        for filter_field in filter_fields:
            key = filter_field[0]
            op_or_value = filter_field[1]
            if filter_field[2] == "None":
                # op_or_value is a value
                filter_statement[key] = op_or_value
            else:
                # op_or_value is an operator
                if filter_field[3] == 1:
                    value = eval(filter_field[2])
                else:
                    value = filter_field[2]
                filter_statement[key] = {op_or_value: value}
        return filter_statement

    @classmethod
    def configuration_schema(cls) -> SourceConfigSchema:
        return SourceMongoDBConfigSchema()


class SourceMongoDBConfigSchema(SourceConfigSchema):
    database = fields.String(required=True)
    collection = fields.String(required=True)
    replication_key = fields.String(required=False)
    batch_size = fields.Int(required=True, strict=True)
    filter_fields = fields.List(required=False, cls_or_instance=fields.Field())
    host = fields.String(required=False)
    port = fields.Int(required=False)
    user = fields.String(required=False)
    password = fields.String(required=False)

