import base64
import json
import threading
import logging
import time
from typing import Any
from functools import wraps

import boto3

LOGGER = logging.getLogger(__name__)


def get_secret(name: str, region: str) -> dict:
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    loaded_secret = json.loads(secret)
    return loaded_secret


class PipelineThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exc = None
        self._stop_event = threading.Event()

    def run(self):
        try:
            super().run()
        except BaseException as exc:
            self.exc = exc

    def join(self, timeout=None):
        super().join(timeout=timeout)
        if self.exc:
            raise self.exc

    def stop(self, wait=False, ignore_exc=False):
        self._stop_event.set()
        if wait:
            try:
                self.join()
            except Exception as exc:
                if ignore_exc:
                    return
                raise exc

    def stop_requested(self):
        return self._stop_event.is_set()


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


def rate_limited(max_per_second):
    lock = threading.Lock()
    min_interval = 1.0 / float(max_per_second)

    def decorate(func):
        last_time_called = [0.0]

        @wraps(func)
        def rate_limited_function(*args, **kwargs):
            lock.acquire()
            elapsed = time.clock() - last_time_called[0]
            left_to_wait = min_interval - elapsed

            if left_to_wait > 0:
                time.sleep(left_to_wait)

            lock.release()

            ret = func(*args, **kwargs)
            last_time_called[0] = time.clock()
            return ret

        return rate_limited_function

    return decorate


class Namespace:
    def __init__(self, **kwargs):
        self._lock = threading.Lock()
        self.__namespace = kwargs

    def get(self, key) -> Any:
        with self._lock:
            return self.__namespace[key]

    def set(self, key, value):
        with self._lock:
            self.__namespace[key] = value

    def inc(self, key):
        with self._lock:
            self.__namespace[key] += 1
            return self.__namespace[key]

    def decr(self, key):
        with self._lock:
            self.__namespace[key] -= 1
            return self.__namespace[key]
