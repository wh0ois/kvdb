"""
The records are stored in the disk so we have to encode an in-memory record into raw bytes and decode it back as well. It also decodes metadata portion of the record.

Record represtation:
----------------------------------------
timestamp|key_size|value_size|key|value|
----------------------------------------

In encode(), timestamp is converted to 8 byte float, and K, V size are bot converted to 8 byte integers. The K & V are encoded to ascii.

decode() does the reverse using struct.unpack() 
"""

import struct
from collections import namedtuple
import kvdb_file

METADATA_STRUCT = ">dqq" #a byte layput of a double 'd', 2 long longs 'q' in big endian format '>'
METADATA_BYTE_SIZE = 24

def encode(record):
    key_size = record.keysize
    value_size = record.valuesize
    timestamp = record.timestamp
    key = record.key
    value = record.value

    metadata = struct.pack(METADATA_STRUCT, timestamp, key_size, value_size)
    data = key.encode() + value.encode()
    record_bytes = metadata + data
    return record_bytes

def decode_metadata(data):
    (timestamp, keysize, valuesize) = struct.unpack(METADATA_STRUCT, data)
    return (timestamp, keysize, valuesize)

def decode(data):
    (timestamp, ksize, vsize) = decode_metadata(data[:METADATA_BYTE_SIZE])
    string_data = data[METADATA_BYTE_SIZE:]
    key = string_data[:ksize]
    value = string_data[ksize:]
    return kvdb_file.Record(timestamp, ksize, vsize, key, value)
