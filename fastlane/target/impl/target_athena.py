import logging
import time

from marshmallow import Schema

from fastlane.target.target import Target
from fastlane.utils import glue_table_to_pyarrow_schema, classproperty

import boto3
import pandas as pd

LOGGER = logging.getLogger(__name__)


class TargetAthena(Target):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.database = self.configuration['database']
        self.view_database = self.configuration['view_database']
        self.table = self.configuration['table']
        self.pk = self.configuration.get('pk')
        self.work_group = self.configuration['work_group']
        self.crawler = self.configuration.get('crawler')
        self.use_glue_schema = self.configuration.get('use_glue_schema')

        if self.replication_mode == 'FULL_TABLE':
            self.table_version = int(time.time())
        else:
            self.table_version = None

        self.athena_client = boto3.client('athena')
        self.glue_client = boto3.client('glue')

    def get_table_schema(self):
        glue_table_details = self.glue_client.get_table(DatabaseName=self.database, Name=self.table)
        pyarrow_schema = glue_table_to_pyarrow_schema(
            glue_table_details=glue_table_details
        )
        return pyarrow_schema

    def get_table_location(self) -> str:
        glue_table_details = self.glue_client.get_table(DatabaseName=self.database, Name=self.table)
        return glue_table_details['StorageDescriptor']['Location']

    def get_offset(self):
        query = f'''
            SELECT MAX({pk})
            FROM {self.view_database}.{self.target_table}
            '''

        results = self.execute_query(query_string=query)
        offset = -1
        if results:
            data = results[0]
            offset = int(data)
        return offset

    def finish(self):
        if self.replication_mode == 'FULL_TABLE':
            if self.crawler:
                LOGGER.info(f'Running glue crawler {self.crawler} for {self.table} ...')
                # TODO: run crawler
                pass
            LOGGER.info(f'Re-creating view in {self.view_database} for {self.table} ...')
            new_table_version_view = (
                f"CREATE OR REPLACE VIEW \"{self.view_database}\".\"{self.table}\" AS "
                f"SELECT o.* FROM \"{self.database}\".\"{self.table}\" o "
                f"JOIN ( "
                f"SELECT j.\"{self.pk}\", MAX(\"_op_timestamp\") AS t FROM \"{self.database}\".\"{self.table}\" AS j "
                f"GROUP BY j.\"{self.pk}\" "
                f") m ON m.\"{self.pk}\" = o.\"{self.pk}\" AND o.\"_table_version\" = {self.table_version};"
            )
            LOGGER.info(new_table_version_view)
            self.execute_query(query_string=new_table_version_view)

    def execute_query(self, query_string: str) -> list:
        query_id = self.athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'database': self.database,
                'catalog': 'AwsDataCatalog'
            },
            WorkGroup=self.work_group
        )['QueryExecutionId']

        poll = True
        retry_interval = 1
        while poll:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_id
            )
            state = response['QueryExecution']['Status']['State']
            if state == 'SUCCEEDED':
                poll = False
            elif state == 'FAILED':
                error_message = f'query with id: {query_id} failed with error: {response}'
                LOGGER.error(error_message)
                raise Exception(error_message, response)
            else:
                time.sleep(retry_interval)
                retry_interval += 1

        response = self.athena_client.get_query_results(
            QueryExecutionId=query_id
        )
        results = []
        data_list = [row['Data'] for row in response['ResultSet']['Rows']]
        for datum in data_list[1:]:
            results.append([x['VarCharValue'] if 'VarCharValue' in x else None for x in datum])
        return [tuple(x) for x in results]

    def load(self, df: pd.DataFrame):
        raise NotImplementedError

    @classproperty
    def target_type(self):
        raise NotImplementedError

    @classmethod
    def configuration_schema(cls) -> Schema:
        raise NotImplementedError

    @classmethod
    def target_id(cls, configuration: dict):
        raise NotImplementedError
