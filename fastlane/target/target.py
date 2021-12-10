from abc import ABC, abstractmethod
import logging
from threading import Lock

from fastlane.core.stage import Stage
import fastlane.utils as utils

from marshmallow import Schema, RAISE, fields
import pandas as pd

LOGGER = logging.getLogger(__name__)


class Target(Stage, ABC):
    def __init__(self, configuration: dict):
        super().__init__()
        self.replication_mode = configuration['replication_mode']
        self.configuration = configuration['target'][self.target_type]
        self.desired_batch_size = self.configuration['batch_size']
        self.concurrency = self.configuration.get('concurrency', 1)
        self._records_loaded = 0
        self._records_loaded_lock = Lock()

    def _start(self):
        workers = []
        for i in range(self.num_workers):
            worker = utils.PipelineThread(
                target=self._load_worker
            )
            worker.start()
            workers.append(worker)
        return workers

    def _load_worker(self):
        try:
            loadable_df = pd.DataFrame()
            for df in self.consume_input():
                loadable_df = loadable_df.append(df)
                if len(loadable_df) >= self.desired_batch_size:
                    self.load(df=loadable_df)
                    self.records_loaded += len(loadable_df)
                    loadable_df = loadable_df.iloc[0:0]
                del df
            if len(loadable_df) > 0:
                self.load(df=loadable_df)
                self.records_loaded += len(loadable_df)
            del loadable_df
        except Exception as exc:
            LOGGER.error(f'{self.target_type} target worker stopping due to exception: {exc}')
            LOGGER.exception(exc)
            self.exception(exc=exc)

    @abstractmethod
    def load(self, df: pd.DataFrame):
        pass

    @abstractmethod
    def get_offset(self):
        pass

    @abstractmethod
    def finish(self):
        pass

    @property
    def target_table(self):
        return self.configuration.get('table')

    @utils.classproperty
    @abstractmethod
    def target_type(self) -> str:
        pass

    @property
    def num_workers(self):
        return self.concurrency

    @property
    def records_loaded(self):
        with self._records_loaded_lock:
            return self._records_loaded

    @records_loaded.setter
    def records_loaded(self, _records_loaded):
        with self._records_loaded_lock:
            self._records_loaded = _records_loaded

    @classmethod
    @abstractmethod
    def configuration_schema(cls) -> Schema:
        pass

    @classmethod
    @abstractmethod
    def target_id(cls, configuration: dict) -> str:
        pass


class TargetConfigSchema(Schema):
    class Meta:
        unknown = RAISE
    batch_size = fields.Int(required=True)
    concurrency = fields.Int(required=False)
    secret = fields.String(required=False)
    secret_region = fields.String(required=False)
