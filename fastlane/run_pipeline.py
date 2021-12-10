import argparse
import json

import fastlane.pipeline as _pipeline
from fastlane.source import get_source
from fastlane.transform import get_transform
from fastlane.target import get_target
from fastlane.utils import validate_config


def driver(source_type: str, transform_type: str, target_type: str, configuration_file: str, **kwargs):
    source = get_source(source_type=source_type)
    transform = get_transform(transform_type=transform_type)
    target = get_target(target_type=target_type)

    with open(configuration_file) as f:
        configuration = json.loads(f.read())

    validate_config(
        configuration=configuration,
        source_cls=source,
        transform_cls=transform,
        target_cls=target
    )

    pipeline = _pipeline.Pipeline(
        source=source,
        transform=transform,
        target=target,
        configuration=configuration,
        **kwargs
    )

    pipeline.run()


def main():
    args = argparse.ArgumentParser(prog='etl-pipeline',
                                   description='Run a ETL pipeline.')
    args.add_argument('--test_source', help='Source type', choices=['mysql', 'bigquery', 'mongodb'])
    args.add_argument('--transform', help='Transform type', choices=['default'], default='default')
    args.add_argument('--target', help='Target type', choices=['s3', 'mysql', 'influxdb', 'firehose'])
    args.add_argument('--config', help='Path to pipeline JSON configuration file')
    params = args.parse_args()
    driver(
        source_type=params.source,
        transform_type=params.transform,
        target_type=params.target,
        configuration_file=params.config
    )


if __name__ == '__main__':
    main()
