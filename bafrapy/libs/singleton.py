import threading
from collections import defaultdict
from typing import Any, DefaultDict
from abc import ABCMeta


class Singleton(ABCMeta):
    _instances: DefaultDict[type, Any] = defaultdict(lambda: None)
    _locks: DefaultDict[type, threading.Lock] = defaultdict(threading.Lock)

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._get_lock(cls):
                if cls not in cls._instances:
                    cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

    @classmethod
    def _get_lock(cls, key):
        if key not in cls._locks:
            cls._locks[key] = threading.Lock()
        return cls._locks[key]
