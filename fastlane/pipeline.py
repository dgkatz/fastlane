import logging
from typing import Type
from contextlib import contextmanager
from datetime import datetime
import sys

from fastlane.source import Source
from fastlane.transform import Transform
from fastlane.target import Target
from fastlane.monitoring.pipeline_monitor import (
    RPSMonitor,
    MemoryMonitor,
    RecordsLoadedMonitor,
    ExitPipelineMonitor,
    QueueMonitor
)
from fastlane.utils import get_secret
from fastlane.api_client.client import PipelineAPI

import watchtower
import slacker_log_handler

LOGGER = logging.getLogger(__name__)


class Pipeline:
    def __init__(self,
                 source: Type[Source],
                 transform: Type[Transform],
                 target: Type[Target],
                 configuration: dict,
                 **kwargs):
        self.configuration = configuration
        self.kwargs = kwargs

        self.pipeline_id = f"{target.target_id(configuration=configuration)}.{source.source_type}.{target.target_type}"

        self.custom_logging_handlers = self.configure_logging()

        # Initialize source stage
        self.source_stage = source(configuration=configuration)
        # Initialize transform stage to consume output from source stage
        self.transform_stage = transform(configuration=configuration)
        self.transform_stage.consume_stage(input_stage=self.source_stage)
        # Initialize target stage to consume output from transform stage
        self.target_stage = target(configuration=configuration)
        self.target_stage.consume_stage(input_stage=self.transform_stage)

        self.replication_mode = self.configuration['replication_mode']

        self.source_stage.offset = self.target_stage.get_offset() if self.replication_mode == 'INCREMENTAL' else -1

        self.pipeline_api_client = SSPipelineAPI()
        pipeline = self.pipeline_api_client.get_pipeline(pipeline_id=self.pipeline_id)
        if not pipeline:
            self.pipeline_api_client.post_pipeline(
                pipeline_id=self.pipeline_id,
                source_type=self.source_stage.source_type,
                target_type=self.target_stage.target_type,
                pipeline_type=self.replication_mode,
                config=self.configuration
            )
            pass

        self.pipeline_run_id = None

    def run(self):
        self.pipeline_run_id = self.pipeline_api_client.create_new_pipeline_run(
            pipeline_id=self.pipeline_id,
            cloudwatch_log_stream=(self.custom_logging_handlers['cloudwatch_handler'].stream_name
                                   if 'cloudwatch_handler' in self.custom_logging_handlers else ''),
            started=datetime.now(),
            start_offset=str(self.source_stage.offset)
        )

        self.source_stage.run()
        self.transform_stage.run()
        self.target_stage.run()

        with self.monitor():
            source_exceptions = self.source_stage.wait_until_done()
            transform_exceptions = self.transform_stage.wait_until_done()
            target_exceptions = self.target_stage.wait_until_done()

        if source_exceptions or transform_exceptions or target_exceptions:
            status = 'failed'
        else:
            status = 'completed'

        self.pipeline_api_client.update_pipeline_run_status(
            pipeline_run_id=self.pipeline_run_id,
            status=status
        )
        LOGGER.info(f'{status} pipeline {self.pipeline_id} with run id {self.pipeline_run_id}')
        LOGGER.info(f'loaded {self.target_stage.records_loaded} records')

    @contextmanager
    def monitor(self):
        memory_monitor = MemoryMonitor(pipeline=self)
        rps_monitor = RPSMonitor(pipeline=self)
        queue_monitor = QueueMonitor(pipeline=self)
        records_loaded_monitor = RecordsLoadedMonitor(pipeline=self)
        exit_pipeline_monitor = ExitPipelineMonitor(pipeline=self)

        LOGGER.info('Starting pipeline monitors')
        memory_monitor.start()
        rps_monitor.start()
        queue_monitor.start()
        records_loaded_monitor.start()
        exit_pipeline_monitor.start()
        try:
            yield True
        finally:
            LOGGER.info('Stopping pipeline monitors')
            memory_monitor.stop(wait=True)
            rps_monitor.stop(wait=True)
            queue_monitor.stop(wait=True)
            records_loaded_monitor.stop(wait=True)
            exit_pipeline_monitor.stop(wait=True)

    def configure_logging(self):
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)

        log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(log_formatter)
        root_logger.addHandler(ch)

        custom_handlers = {}

        logs_config = self.configuration.get('logs')

        logs_to_file = logs_config.get('log_file')
        if logs_to_file:
            file_handler = logging.FileHandler(filename=logs_to_file)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(log_formatter)
            root_logger.addHandler(file_handler)
            custom_handlers['file_handler'] = file_handler

        log_to_cloudwatch = logs_config.get('log_to_cloudwatch')
        if log_to_cloudwatch:
            cloudwatch_handler = watchtower.CloudWatchLogHandler(
                log_group=self.pipeline_id,
                stream_name=datetime.strftime(datetime.now(), '%Y.%m.%d-%H.%M'),
                send_interval=30
            )
            cloudwatch_handler.setLevel(logging.INFO)
            cloudwatch_handler.setFormatter(log_formatter)
            root_logger.addHandler(cloudwatch_handler)
            custom_handlers['cloudwatch_handler'] = cloudwatch_handler

        error_log_to_slack = logs_config.get('error_log_to_slack')
        if error_log_to_slack:
            slack_secret_name = self.kwargs.get('slack_secret')
            assert slack_secret_name
            slack_secret = get_secret(name=slack_secret_name, region='us-east-1')
            slack_handler = slacker_log_handler.SlackerLogHandler(
                api_key=slack_secret.get('channel_token'),
                channel=slack_secret.get('channel_name'),
                username='SS Pipeline Bot'
            )
            slack_handler.setLevel(logging.ERROR)
            slack_handler.setFormatter(log_formatter)
            root_logger.addHandler(slack_handler)
            custom_handlers['slack_handler'] = slack_handler

        return custom_handlers
