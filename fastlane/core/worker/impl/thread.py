from threading import Thread, Event

from fastlane.core.worker import AbstractWorker


class ThreadWorker(AbstractWorker, Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exc = None
        self._stop_event = Event()

    def start(self):
        self.start()

    def stop(self, wait=False):
        self._stop_event.set()
        if wait:
            try:
                self.join()
            except Exception as exc:
                raise exc

    def is_running(self):
        return self.is_alive()

    def stop_requested(self):
        return self._stop_event.is_set()
