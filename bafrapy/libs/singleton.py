import threading

class Singleton(type):
    _instances = {}
    _locks = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            print("Creating instance")
            with cls._get_lock(cls):
                if cls not in cls._instances:
                    cls._instances[cls] = super().__call__(*args, **kwargs)
        else:
            print("instance is created")
        return cls._instances[cls]

    @classmethod
    def _get_lock(cls, key):
        if key not in cls._locks:
            cls._locks[key] = threading.Lock()
        return cls._locks[key]
