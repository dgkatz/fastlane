from abc import ABC, abstractmethod
from typing import List, Optional
import logging
import threading
import time

from fastlane.core.queue import IterableQueue, get_queue_type
from fastlane.utils import PipelineThread

LOGGER = logging.getLogger(__name__)


class PipelineException(Exception):
    """Pipeline exception"""


class Stage(ABC):
    def __init__(self, queue_type: str):
        self._input_queue: Optional[IterableQueue] = None
        self._output_queue: Optional[IterableQueue] = None
        self._queue_type = get_queue_type(queue_type=queue_type)
        self.exceptions = []
        self.workers: List[PipelineThread] = []
        self._items_produced = 0
        self._items_consumed = 0
        self._items_produced_lock = threading.Lock()
        self._items_consumed_lock = threading.Lock()

    def produce_stage(self, output_stage: "Stage"):
        queue = IterableQueue(
            max_size=output_stage.num_workers,
            total_sources=self.num_workers,
            queue_class=self._queue_type
        )
        self._output_queue = queue
        output_stage._input_queue = queue

    def consume_stage(self, input_stage: "Stage"):
        queue = IterableQueue(
            max_size=self.num_workers,
            total_sources=input_stage.num_workers,
            queue_class=self._queue_type
        )
        self._input_queue = queue
        input_stage._output_queue = queue

    def consume_input(self):
        for item in self._input_queue:
            self.items_consumed += len(item)
            yield item

    def consume_output(self):
        """Convenience function for testing, not used by stages."""
        for item in self._output_queue:
            yield item

    def is_work_done(self):
        if self._input_queue:
            return self._input_queue.is_done()
        else:
            return True

    def is_running(self):
        if not self.workers:
            return False
        return any(worker.is_alive() for worker in self.workers)

    def is_done(self):
        return self.is_work_done() and not self.is_running()

    def producer_done(self):
        self._output_queue.producer_done()

    def stop(self, exc: Exception = None):
        if self._input_queue:
            self._input_queue.kill(exc=exc)
        if self._output_queue:
            self._output_queue.kill(exc=exc)

    def run(self):
        self.workers = self._start()
        return self.workers

    def produce(self, item):
        self._output_queue.put(item)
        self.items_produced += len(item)

    @property
    def items_produced(self) -> int:
        with self._items_produced_lock:
            return self._items_produced

    @items_produced.setter
    def items_produced(self, value: int):
        with self._items_produced_lock:
            self._items_produced = value

    @property
    def items_consumed(self) -> int:
        with self._items_consumed_lock:
            return self._items_consumed

    @items_consumed.setter
    def items_consumed(self, value: int):
        with self._items_consumed_lock:
            self._items_consumed = value

    def wait_until_done(self):
        while self.is_running():
            time.sleep(1)
        self.finish()
        return self.exceptions

    def exception(self, exc: Exception):
        self.stop(exc=exc)
        self.exceptions.append(exc)

    @abstractmethod
    def _start(self) -> List[PipelineThread]:
        pass

    @property
    @abstractmethod
    def num_workers(self) -> int:
        return 1

    @abstractmethod
    def finish(self):
        return
