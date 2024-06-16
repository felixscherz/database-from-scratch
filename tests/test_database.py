import struct
from io import BytesIO
from typing import BinaryIO
from typing import Sequence

from database import FILE_HEADER_MAGIC
from database import MAGIC_BYTES
from database import Database


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
    assert schema == db.schema("greetings")


def test_read_primary_key_for_existing_table():
    db = Database()
    schema = {"id": int, "greeting": str}
    db.create_table("greetings", schema=schema, primary_key="id")
    assert ("id",) == db.primary_key("greetings")


def test_read_primary_key_sequence_for_existing_table():
    db = Database()
    schema = {"id": int, "greeting": str, "info": str}
    db.create_table("greetings", schema=schema, primary_key=("id", "info"))
    assert ("id", "info") == db.primary_key("greetings")


def test_tuple():
    assert isinstance("inin", Sequence)
