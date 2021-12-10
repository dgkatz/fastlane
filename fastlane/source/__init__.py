from typing import Type

from fastlane.source.source import Source, SourceConfigSchema
from fastlane.source.impl.source_mysql import SourceMySQL
from fastlane.source.impl.source_bigquery import SourceBigQuery
from fastlane.source.impl.source_mongo import SourceMongoDB


def get_source(source_type: str) -> Type[Source]:
    if source_type == 'mysql':
        return SourceMySQL
    elif source_type == 'bigquery':
        return SourceBigQuery
    elif source_type == 'mongodb':
        return SourceMongoDB
    else:
        raise NotImplementedError()
