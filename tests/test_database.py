from io import BytesIO
import struct
from typing import BinaryIO
from database import Database,MAGIC_BYTES, FILE_HEADER_MAGIC


def test_read_after_write():
    db = Database()
    schema = {"id": int, "greeting": str}
    db.create_table("greetings", schema=schema, primary_key="id")
    db.table_info("greetings")
    db.insert((0, "Hello World"))

    assert db.read(0) == ("Hello World")

def test_struct():
    a = FILE_HEADER_MAGIC.pack(*(b"m", b"a", b"g", b"i", b"c"))
    print(a)

    a = FILE_HEADER_MAGIC.pack(*(c.encode() for c in "magic"))

    out = FILE_HEADER_MAGIC.unpack(a)
    print(out)


def test_buffer():
    a = struct.pack("<b", 3)
    out = struct.unpack("<b", a)
    print(out)

def test_read_schema_for_existing_table():
    db = Database()
    schema = {"id": int, "greeting": str}
    db.create_table("greetings", schema=schema, primary_key="id")
    metadata = db.table_info("greetings")
    assert metadata.schema == schema

