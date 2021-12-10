from typing import Type

from fastlane.target.target import Target, TargetConfigSchema
from fastlane.target.impl.target_s3 import TargetS3
from fastlane.target.impl.target_mysql import TargetMySQL
from fastlane.target.impl.target_influx import TargetInfluxDB
from fastlane.target.impl.target_firehose import TargetFirehose
from fastlane.target.impl.target_athena import TargetAthena


def get_target(target_type: str) -> Type[Target]:
    if target_type == 's3':
        return TargetS3
    elif target_type == 'influxdb':
        return TargetInfluxDB
    elif target_type == 'mysql':
        return TargetMySQL
    elif target_type == 'firehose':
        return TargetFirehose
    else:
        raise NotImplementedError()
