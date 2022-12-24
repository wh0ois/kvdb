from cmath import e
import time
"""
To read and write a file

A file represents a log of key, value pairs alongwith its metadata (size and timestamp)

write() encodes and writes data to a file
read() read files and decode
"""

import uuid
import uuid
import codec
from collections import namedtuple

Record = namedtuple(
    'Record', ['timestamp','keysize', 'valuesize', 'key', 'value']
)

class File:

    def __init__(self, dir, filename=str(uuid.uuid4()), offset=0):
        self.filename = '/'.join([dir, filename])
        self. offset = offset

    def _load_next_record(self):
        read_bytes = 0
        with open(self.filename, 'rb') as f:
            f.seek(self.offset, 0)
            meta_bytes = f.read(codec.METADATA_BYTE_SIZE)
            if meta_bytes:
                (tstamp, ksize, vsize) = codec.decode_metadata(meta_bytes)
                key_bytes= f.read(ksize)
                value_bytes = f.read(vsize)
                key = key_bytes.decode()
                value = value_bytes.decode()

                read_bytes += len(meta_bytes) + ksize + vsize
                self.offset += read_bytes
                return Record(tstamp, ksize, vsize, key, value)
    
    def write(self, key, value):
        keysize = len(key)
        valuesize = len(value)
        timestamp = time.time()
        record = Record(timestamp, keysize, valuesize, key, value)
        data = codec.encode(record)
        count = 0
        with open(self.filename, 'ab') as f:
            count = f.write(data)
        curr_offset = self.offset
        self.offset += count
        return (timestamp, curr_offset, count)

    def read(self, pos, size):
        data = b''
        with open(self.filename, 'rb') as f:
            f.seek(pos, 0)
            data = f.read(size)
        return codec.decode(data).value
