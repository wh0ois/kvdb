"""
The records are stored in the disk so we have to encode an in-memory record into raw bytes and decode it back as well. It also decodes metadata portion of the record.

Record represtation:
----------------------------------------
timestamp|key_size|value_size|key|value|
----------------------------------------

In encode(), timestamp is converted to 8 byte float, and K, V size are bot converted to 8 byte integers. The K & V are encoded to ascii.

decode() does the reverse using struct.unpack() 
"""
