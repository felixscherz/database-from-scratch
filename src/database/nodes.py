from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import BinaryIO

from database.types import Datatype


class DecodeError(Exception):
    def __init__(self, offset: int):
        super().__init__(f"Invalid bytes encountered at {offset=}")


MAGIC_BYTES = tuple(c.encode() for c in "magic")
VERSION_BYTES = (0, 0, 1)
BYTES_PER_PAGE = 2**12  # 4096

FILE_HEADER_MAGIC = struct.Struct("<5c3x")
VERSION_BYTES_STRUCT = struct.Struct("<3i")
BYTES_PER_PAGE_STRUCT = struct.Struct("<I")
NUMBER_OF_PAGES_STRUCT = struct.Struct("<Q")

# datatypes are encoded as different bytes
NUMBER_OF_COLUMNS_STRUCT = struct.Struct("<b")
DATATYPE_STRUCT = struct.Struct("<b")

DATATYPE_TO_BYTE = {int: 0, float: 1, str: 2, bytes: 3}
BYTE_TO_DATATYPE = {0: int, 1: float, 2: str, 3: bytes}

TYPE_OF_PAGE_STRUCT = struct.Struct("<b")
INTERNAL_NODE = 0
LEAF_NODE = 1


def read_with(reader: BinaryIO, struct: struct.Struct):
    return struct.unpack(reader.read(struct.size))


@dataclass
class MetaHeader:
    magic: tuple[bytes, ...]
    version: tuple[int, int, int]
    bytes_per_page: int
    number_of_pages: int
    schema: dict[str, type[Datatype]]
    primary_key: tuple[str, ...]

    def write(self, writer: BinaryIO) -> int:
        key_mapping = {name: i for i, name in enumerate(self.primary_key)}
        start = writer.tell()
        writer.write(FILE_HEADER_MAGIC.pack(*self.magic))
        writer.write(VERSION_BYTES_STRUCT.pack(*self.version))
        writer.write(BYTES_PER_PAGE_STRUCT.pack(self.bytes_per_page))
        writer.write(NUMBER_OF_PAGES_STRUCT.pack(self.number_of_pages))
        writer.write(NUMBER_OF_COLUMNS_STRUCT.pack(len(self.schema)))
        for name, datatype in self.schema.items():
            # <length-of-name><name><is-primary-key><datatype>
            writer.write(struct.pack("<b", len(name)))
            writer.write(struct.pack(f"<{len(name)}s", name.encode()))
            # this could be changed to encode the key position 0,1,2,3 to allow for composite keys
            writer.write(struct.pack("<?", name in self.primary_key))
            if name in self.primary_key:
                writer.write(struct.pack("<B", key_mapping[name]))
            writer.write(DATATYPE_STRUCT.pack(DATATYPE_TO_BYTE[datatype]))
        return writer.tell() - start

    @classmethod
    def read(cls, reader: BinaryIO) -> MetaHeader:
        if not read_with(reader, FILE_HEADER_MAGIC) == MAGIC_BYTES:
            raise DecodeError(0)
        version = read_with(reader, VERSION_BYTES_STRUCT)
        bytes_per_page = read_with(reader, BYTES_PER_PAGE_STRUCT)[0]
        number_of_pages = read_with(reader, NUMBER_OF_PAGES_STRUCT)[0]
        number_of_columns = read_with(reader, NUMBER_OF_COLUMNS_STRUCT)[0]
        schema = {}
        key_mapping = {}
        for _ in range(number_of_columns):
            name_length = read_with(reader, struct.Struct("<b"))
            name = read_with(reader, struct.Struct(f"<{name_length[0]}s"))
            is_primary_key = read_with(reader, struct.Struct("<?"))
            if is_primary_key[0]:
                key_index = read_with(reader, struct.Struct("<B"))
                key_mapping[key_index[0]] = name[0].decode()
            datatype = read_with(reader, DATATYPE_STRUCT)
            schema[name[0].decode()] = BYTE_TO_DATATYPE[datatype[0]]
        key_sequence = tuple(key_mapping[i] for i in range(len(key_mapping)))
        return cls(
            magic=MAGIC_BYTES,
            version=version,
            bytes_per_page=bytes_per_page,
            number_of_pages=number_of_pages,
            schema=schema,
            primary_key=key_sequence,
        )


class Header: ...


class InternalNode: ...


class LeafNode:
    header: None

    ...
