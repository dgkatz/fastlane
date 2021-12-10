from abc import ABC, abstractmethod


class AbstractWorker(ABC):
    @abstractmethod
    def start(self, *args, **kwargs):
        pass

    @abstractmethod
    def stop(self, *args, **kwargs):
        pass

    @abstractmethod
    def is_running(self):
        pass
