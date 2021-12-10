import logging
import json

from fastlane.source import Source, SourceConfigSchema
import fastlane.utils as utils

from marshmallow import fields
from google.oauth2 import service_account
import google.cloud.bigquery_storage as bigquery_storage

LOGGER = logging.getLogger(__name__)


class SourceBigQuery(Source):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.offset > 0:
            LOGGER.warning('bigquery source does not support using offset.')
        self.source_secret = self.configuration[self.source_type]['secret']
        self.source_secret_region = self.configuration['ssm']['region']
        big_query_secret = utils.get_secret(name=self.source_secret, region=self.source_secret_region)
        service_account_json = json.loads(big_query_secret['service_account_json'])
        self._gcp_credentials = service_account.Credentials.from_service_account_info(
            info=service_account_json,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        self.read_client = bigquery_storage.BigQueryReadClient(credentials=self._gcp_credentials)
        self.gcp_project = self.configuration[self.source_type]['gcp_project']
        self.dataset_id = self.configuration[self.source_type]['dataset_id']
        self.table_id = self.configuration[self.source_type]['table_id']
        self.table = f"projects/{self.gcp_project}/datasets/{self.dataset_id}/tables/{self.table_id}"

        self._requested_session = bigquery_storage.ReadSession()
        self._requested_session.table = self.table
        self._requested_session.data_format = bigquery_storage.DataFormat.AVRO

        parent = "projects/{}".format(self.gcp_project)
        session = self.read_client.create_read_session(
            parent=parent,
            read_session=self._requested_session,
            max_stream_count=self.num_workers,
        )
        self._row_readers = [self.read_client.read_rows(stream.name).rows(session) for stream in session.streams]

    def extract(self, i: int):
        try:
            row_page = next(self._row_readers[i].pages)
            return list(row_page)
        except IndexError:
            return []

    def finish(self):
        return

    @utils.classproperty
    def source_type(self):
        return 'bigquery'

    @classmethod
    def configuration_schema(cls) -> SourceConfigSchema:
        return SourceBigQueryConfigSchema()


class SourceBigQueryConfigSchema(SourceConfigSchema):
    gcp_project = fields.String(required=True)
    dataset_id = fields.String(required=True)
    table_id = fields.String(required=False)
