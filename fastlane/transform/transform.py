from abc import ABC, abstractmethod
import logging
import time

from fastlane.core.stage import Stage
import fastlane.utils as utils

from marshmallow import Schema, RAISE, fields
import pandas as pd

LOGGER = logging.getLogger(__name__)


class Transform(Stage, ABC):
    def __init__(self, configuration: dict):
        super().__init__()
        self.configuration = configuration.get('transform', {})
        self.should_add_op_fields = self.configuration.get('add_op_fields', True)
        self.concurrency = self.configuration.get('concurrency', 1)

    def _start(self):
        workers = []
        for i in range(self.num_workers):
            worker = utils.PipelineThread(
                target=self._transform_worker
            )
            worker.start()
            workers.append(worker)
        return workers

    def _transform_worker(self):
        try:
            for record_batch in self.consume_input():
                if not record_batch:
                    continue
                df = pd.DataFrame(record_batch)
                if self.should_add_op_fields:
                    df = self.add_op_fields(df=df)
                df = self.transform(df=df)
                self.produce(df)
            self.producer_done()
        except Exception as exc:
            LOGGER.error(f'{self.transform_type} transform worker stopping due to exception: {exc}')
            LOGGER.exception(exc)
            self.exception(exc=exc)

    @staticmethod
    def add_op_fields(df: pd.DataFrame) -> pd.DataFrame:
        df['_op_type'] = 'i'
        df['_op_timestamp'] = int(time.time() * 1000)
        return df

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    @utils.classproperty
    @abstractmethod
    def transform_type(self) -> str:
        pass

    @property
    def num_workers(self):
        return self.concurrency

    def finish(self):
        super().finish()

    @classmethod
    @abstractmethod
    def configuration_schema(cls) -> Schema:
        pass


class TransformConfigSchema(Schema):
    class Meta:
        unknown = RAISE
    add_op_fields = fields.Bool(required=False)
    concurrency = fields.Int(required=False)
