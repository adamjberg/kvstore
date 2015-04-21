class KVStore:
    MAX_SPACE_AVAILABLE = 64 * 1024 * 1024
    MAX_NUM_KEYS = 100000
    NUMBER_LOCATIONS = 256

    def __init__(self):
        self.space_available = KVStore.MAX_SPACE_AVAILABLE
        self.kv_dict = {}

        for i in range(KVStore.NUMBER_LOCATIONS):
            self.kv_dict[i] = {}

        self.num_keys = 0

    def put(self, location, key, value):
        self.remove(location, key)

        if len(value) > self.space_available or self.num_keys >= KVStore.MAX_NUM_KEYS:
            return False

        self.kv_dict[location][key] = value
        self.space_available -= len(value)
        self.num_keys += 1

        return True

    def get(self, location, key):
        return self.kv_dict[location].get(key, None)

    def remove(self, location, key):
        value = self.kv_dict[location].pop(key, None)
        if value is not None:
            self.space_available += len(value)
            self.num_keys -= 1

        return value is not None