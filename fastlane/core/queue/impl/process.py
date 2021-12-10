from queue import Full, Empty
from multiprocessing import Queue

from fastlane.core.queue import AbstractQueue, QueueFull, QueueEmpty

import pandas as pd


class ProcessQueue(Queue, AbstractQueue):
    def __init__(self):
        super().__init__()

    def get(self, block: bool = True, timeout: float = 0.02):
        try:
            return super().get(block=block, timeout=timeout)
        except Empty:
            raise QueueEmpty()

    def put(self, df: pd.DataFrame, block: bool = True, timeout: float = None):
        try:
            return super().put(obj=df, block=block, timeout=timeout)
        except Full:
            raise QueueFull()

    def clear(self):
        try:
            while True:
                super().get_nowait()
        except Empty:
            pass
