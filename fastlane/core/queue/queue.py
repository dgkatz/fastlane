from abc import ABC, abstractmethod
from typing import Optional, Type

from fastlane.utils import Namespace

import pandas as pd


class QueueFull(Exception):
    """Queue is full."""


class QueueEmpty(Exception):
    """Queue is empty."""


class QueueStopped(Exception):
    """Queue stopped."""


class AbstractQueue(ABC):

    @abstractmethod
    def get(self, block: bool = True, timeout: float = 0.02) -> Optional[pd.DataFrame]:
        pass

    @abstractmethod
    def put(self, df: Optional[pd.DataFrame], block: bool = True, timeout: float = None):
        pass

    @abstractmethod
    def clear(self):
        pass

    @abstractmethod
    def empty(self) -> bool:
        pass


class IterableQueue:
    def __init__(self, queue_class: Type[AbstractQueue], max_size: int = 1, total_sources: int = 1):
        super().__init__(maxsize=max_size)
        self.namespace = Namespace(
            exception=False, stop_requested=False, remaining=total_sources
        )
        self.queue = queue_class()

    def get(self, block: bool = True, timeout: float = 0.02) -> Optional[pd.DataFrame]:
        """Will return when a real value (not None) is retrieved or queue is empty"""
        while True:
            x = self.queue.get(block=block, timeout=timeout)
            if x is None:
                self.namespace.decr('remaining')
                continue
            return x

    def put(self, item: Optional[pd.DataFrame], block: bool = True, timeout: float = None):
        if self.is_done():
            raise QueueStopped('Output queue was stopped either because consumers are no longer running.')
        if block and timeout is None:
            while True:
                try:
                    self.queue.put(df=item, block=True, timeout=0.02)
                    return
                except QueueFull:
                    pass
        else:
            return self.queue.put(df=item, block=False, timeout=timeout)

    def clear(self):
        self.queue.clear()

    def is_done(self):
        return self.namespace.get('stop_requested') or (self.namespace.get('remaining') <= 0 and self.queue.empty())

    def stop(self):
        self.namespace.set('remaining', 0)
        self.clear()

    def kill(self, exc=None):
        self.namespace.set('remaining', 0)
        self.namespace.set('stop_requested', True)
        if exc:
            self.namespace.set('exception', True)
        self.clear()

    def producer_done(self):
        self.put(None)

    def __iter__(self):
        """Will continue iterating until the queue is empty and all sentinel flags have been processed.
        Or a stop request was received"""
        while not self.is_done():
            try:
                x = self.get(timeout=0.02)
            except QueueEmpty:
                continue
            yield x
