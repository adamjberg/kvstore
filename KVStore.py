from Node import Node

class KVStore:
    MAX_SPACE_AVAILABLE = 64 * 1024 * 1024
    MAX_NUM_KEYS = 100000

    def __init__(self):
        self.space_available = KVStore.MAX_SPACE_AVAILABLE
        self.kv_dict = dict()
        self.nodes = dict()

    def put(self, key, value):
        self.remove(key)

        if len(value) > self.space_available or len(self.kv_dict) >= KVStore.MAX_NUM_KEYS:
            return False

        self.kv_dict[key] = value
        self.space_available -= len(value)

        return True

    def get(self, key):
        return self.kv_dict.get(key, None)

    def remove(self, key):
        value = self.kv_dict.pop(key, None)
        if value is not None:
            self.space_available += len(value)

        return value is not None