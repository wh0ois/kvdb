"""
index data structure
"""

class KeyDir:
    def __init__(self):
        self.items = {}

    def put(self, file, timestamp, k, v, offset, size):
        self.items[k] = KeyDirItem(file, timestamp, v, offset, size)

    def get(self, k):
        if k in self.items:
            return self.items[k]

    def delete(self, k):
        del self.items[k]


class KeyDirItem:
    def __init__(self, file, timestamp, v, offset, size):
        self.file_id = file
        self.timestamp = timestamp
        self.value = v
        self.size = size
        self.pos = offset
