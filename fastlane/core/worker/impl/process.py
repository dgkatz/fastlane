from multiprocessing import Process

from fastlane.core.worker import AbstractWorker


class ProcessWorker(AbstractWorker, Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        self.start()

    def stop(self, wait=False):
        self.terminate()
        if wait:
            self.join()

    def is_running(self):
        return self.is_alive()
