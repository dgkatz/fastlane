import logging
from queue import Queue
import time
import psutil
import os
import collections

from fastlane.utils import PipelineThread

LOGGER = logging.getLogger(__name__)


class MemoryMonitor(PipelineThread):
    def __init__(self, pipeline):
        super().__init__()
        self.pipeline = pipeline

    def run(self):
        pid = os.getpid()
        proc = psutil.Process(pid)
        iteration = 0
        while not self.stop_requested():
            iteration += 1
            time.sleep(5)
            memory_usage = proc.memory_info().rss
            LOGGER.info(f'pipeline memory usage: {memory_usage:,}')
            if iteration == 6:
                # Every 30 seconds send to API
                self.pipeline.pipeline_api_client.post_memory_usage(
                    pipeline_run_id=self.pipeline.pipeline_run_id,
                    memory_usage=(memory_usage/1000000)
                )
                iteration = 0


class QueueMonitor(PipelineThread):
    def __init__(self, pipeline):
        super().__init__()
        self.pipeline = pipeline

    def run(self, *args, **kwargs):
        while not self.stop_requested():
            time.sleep(5)
            queue_counts = 'queue counts: '
            queue_counts += f'source =|{self.pipeline.source_stage._output_queue.qsize()}|=> '
            queue_counts += f'transform =|{self.pipeline.transform_stage._output_queue.qsize()}|=> '
            queue_counts += 'target'
            LOGGER.info(queue_counts)


class RPSMonitor(PipelineThread):
    def __init__(self, pipeline):
        super().__init__()
        self.pipeline = pipeline

    def run(self, *args, **kwargs):
        last_5_rps = collections.deque(maxlen=5)
        records_loaded = self.pipeline.target_stage.records_loaded
        iteration = 0
        while not self.stop_requested():
            iteration += 1
            time.sleep(5)
            latest_records_loaded = self.pipeline.target_stage.records_loaded
            new_records_loaded = latest_records_loaded - records_loaded
            records_loaded = latest_records_loaded
            latest_rps = new_records_loaded/5
            last_5_rps.append(latest_rps)
            avg_rps = sum(last_5_rps)/5
            LOGGER.info(f'pipeline rps: {avg_rps}')
            if iteration == 6:
                # Every 30 seconds send to API
                self.pipeline.pipeline_api_client.post_rps(
                    pipeline_run_id=self.pipeline.pipeline_run_id,
                    rps=avg_rps
                )
                iteration = 0


class RecordsLoadedMonitor(PipelineThread):
    def __init__(self, pipeline):
        super().__init__()
        self.pipeline = pipeline

    def run(self, *args, **kwargs):
        iteration = 0
        while not self.stop_requested():
            records_loaded = self.pipeline.target_stage.records_loaded
            iteration += 1
            time.sleep(5)
            LOGGER.info(f'pipeline records loaded: {records_loaded}')
            if iteration == 6:
                # Every 30 seconds send to API
                self.pipeline.pipeline_api_client.update_pipeline_run_records_loaded(
                    pipeline_run_id=self.pipeline.pipeline_run_id,
                    records_loaded=records_loaded
                )
                iteration = 0


class ExitPipelineMonitor(PipelineThread):
    def __init__(self, pipeline):
        super().__init__()
        self.pipeline = pipeline
        self.exit_file = f'/tmp/fastlane/{self.pipeline.pipeline_id}.exit'
        if not os.path.exists('/tmp/fastlane'):
            os.mkdir('/tmp/fastlane')

    def run(self):
        while not self.stop_requested():
            time.sleep(5)
            if os.path.isfile(self.exit_file):
                exc = Exception(f'Exit file detected {self.exit_file}, stopping pipeline gracefully.')
                LOGGER.exception(exc)
                # This will propagate the stop to downstream stages transform and target.
                self.pipeline.source_stage.source_stage.stop()
