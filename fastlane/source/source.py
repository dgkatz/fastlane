from abc import ABC, abstractmethod
import logging
from typing import List

from fastlane.core.stage import Stage
import fastlane.utils as utils

from marshmallow import Schema, RAISE, fields

LOGGER = logging.getLogger(__name__)


class Source(Stage, ABC):
    def __init__(self, configuration: dict):
        super().__init__()
        self.replication_mode = configuration['replication_mode']
        self.configuration = configuration['source'][self.source_type]
        self.offset = self.configuration['offset']
        self.concurrency = self.configuration.get('concurrency', 1)
        if self.concurrency != 1:
            raise NotImplementedError('Concurrency has not been implemented in source.')

    def _start(self):
        workers = []
        for i in range(self.num_workers):
            worker = utils.PipelineThread(
                target=self._extract_worker,
                args=(i,)
            )
            worker.start()
            workers.append(worker)
        return workers

    def _extract_worker(self, i: int):
        try:
            while True:
                records_batch = self.extract(i=i)
                if len(records_batch) == 0:
                    break
                self.produce(records_batch)
            self.producer_done()
        except Exception as exc:
            LOGGER.error(f'{self.source_type} source worker stopping due to exception:')
            LOGGER.exception(exc)
            self.exception(exc=exc)

    @abstractmethod
    def extract(self, i: int) -> List[dict]:
        pass

    @abstractmethod
    def finish(self):
        pass

    @utils.classproperty
    @abstractmethod
    def source_type(self) -> str:
        pass

    @classmethod
    @abstractmethod
    def configuration_schema(cls) -> Schema:
        pass

    @property
    def num_workers(self):
        return self.concurrency


class SourceConfigSchema(Schema):
    class Meta:
        unknown = RAISE
    offset = fields.Int(required=False)
    concurrency = fields.Int(required=False)
    secret = fields.String(required=False)
    secret_region = fields.String(required=False)
