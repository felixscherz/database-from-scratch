from __future__ import annotations

import struct
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from io import BytesIO
from typing import BinaryIO
from typing import ClassVar
from typing import Literal

from database.types import Datatype

__all__ = ["InternalNode", "MetaHeader", "LeafNode"]


class DecodeError(Exception):
    def __init__(self, offset: int):
        super().__init__(f"Invalid bytes encountered at {offset=}")


MAGIC_BYTES = b"magic"
VERSION_BYTES = (0, 0, 1)
BYTES_PER_PAGE = 2**12  # 4096

FILE_HEADER_MAGIC = struct.Struct("<5s3x")
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


class NodeType(Enum):
    INTERNAL = 0
    LEAF = 1


def read_with(reader: BinaryIO, struct: struct.Struct):
    return struct.unpack(reader.read(struct.size))


@dataclass
class MetaHeader:
    number_of_pages: int
    schema: dict[str, type[Datatype]]
    primary_key: tuple[str, ...]
    magic: bytes = field(default=MAGIC_BYTES)
    version: tuple[int, int, int] = field(default=VERSION_BYTES)
    bytes_per_page: int = field(default=BYTES_PER_PAGE)

    def write(self, writer: BinaryIO) -> int:
        key_mapping = {name: i for i, name in enumerate(self.primary_key)}
        start = writer.tell()
        writer.write(FILE_HEADER_MAGIC.pack(self.magic))
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
        if not read_with(reader, FILE_HEADER_MAGIC)[0] == MAGIC_BYTES:
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
            number_of_pages=number_of_pages,
            schema=schema,
            primary_key=key_sequence,
        )

    @property
    def size(self):
        total = 0
        total += FILE_HEADER_MAGIC.size
        total += VERSION_BYTES_STRUCT.size
        total += BYTES_PER_PAGE_STRUCT.size
        total += NUMBER_OF_PAGES_STRUCT.size
        total += NUMBER_OF_COLUMNS_STRUCT.size
        for name in self.schema:
            total += struct.calcsize("<b")
            total += struct.calcsize(f"<{len(name)}s")
            total += struct.calcsize("<?")
            if name in self.primary_key:
                total += struct.calcsize("<B")
            total += DATATYPE_STRUCT.size
        return total


@dataclass
class InternalNode:
    key_mapping: dict[Datatype, int]  # this needs to adapt to support key sequences
    type: Literal[NodeType.INTERNAL] = NodeType.INTERNAL
    meta_header: MetaHeader | None = None
    type_of_page_struct: ClassVar[struct.Struct] = TYPE_OF_PAGE_STRUCT
    number_of_items_struct: ClassVar[struct.Struct] = struct.Struct("<I")
    free_space_struct: ClassVar[struct.Struct] = struct.Struct("<I")
    offset_to_end_struct: ClassVar[struct.Struct] = struct.Struct("<I")
    offset_to_free_struct: ClassVar[struct.Struct] = struct.Struct("<I")
    bytes_per_page: ClassVar[int] = BYTES_PER_PAGE

    @property
    def number_of_items(self) -> int:
        return len(self.key_mapping)

    @property
    def offset_to_end(self) -> int:
        return self.bytes_per_page

    @property
    def offset_to_free(self) -> int:
        return sum(
            (
                self.meta_header.size if self.meta_header else 0,
                self.type_of_page_struct.size,
                self.number_of_items_struct.size,
                self.free_space_struct.size,
                self.offset_to_end_struct.size,
                self.offset_to_free_struct.size,
                len(self._encode_key_mapping()),
            )
        )

    @property
    def free_space(self) -> int:
        return self.bytes_per_page - self.offset_to_free

    def write(self, writer: BinaryIO) -> int:
        start = writer.tell()
        if self.meta_header:
            self.meta_header.write(writer)
        writer.write(self.type_of_page_struct.pack(self.type.value))
        writer.write(self.number_of_items_struct.pack(self.number_of_items))  # number of items on the page
        writer.write(
            self.free_space_struct.pack(self.free_space)
        )  # free space left on the page after accounting for other headers
        writer.write(self.offset_to_end_struct.pack(self.offset_to_end))  # point to end of the first page
        writer.write(self.offset_to_free_struct.pack(self.offset_to_free))  # point to start of free space
        writer.write(self._encode_key_mapping())
        return writer.tell() - start

    def _encode_key_mapping(self) -> bytes:
        buffer = BytesIO()
        for value, offset in self.key_mapping.items():
            buffer.write(DATATYPE_STRUCT.pack(DATATYPE_TO_BYTE[type(value)]))
            match type(value):
                case int():
                    buffer.write(struct.pack("<i", value))
                case float():
                    buffer.write(struct.pack("<f", value))
                case str(), bytes():
                    assert isinstance(value, (str, bytes))
                    buffer.write(struct.pack("<I", len(value)))
                    buffer.write(struct.pack(f"<{len(value)}s", value))
            buffer.write(struct.pack("<I", offset))
        return buffer.getvalue()

    @classmethod
    def read(cls, reader: BinaryIO, root=False) -> InternalNode:
        meta_header = MetaHeader.read(reader) if root else None
        type_of_page = read_with(reader, cls.type_of_page_struct)[0]
        assert type_of_page == NodeType.INTERNAL.value
        number_of_items = read_with(reader, cls.number_of_items_struct)[0]
        free_space = read_with(reader, cls.free_space_struct)[0]
        offset_to_end = read_with(reader, cls.offset_to_end_struct)[0]
        offset_to_free = read_with(reader, cls.offset_to_free_struct)[0]
        key_mapping = {}
        for _ in range(number_of_items):
            datatype = BYTE_TO_DATATYPE[read_with(reader, DATATYPE_STRUCT)[0]]
            match datatype:
                case int():
                    value = read_with(reader, struct.Struct("<i"))[0]
                case float():
                    value = read_with(reader, struct.Struct("<f"))[0]
                case str(), bytes():
                    size = read_with(reader, struct.Struct("<I"))[0]
                    value = read_with(reader, struct.Struct(f"<{size}s"))[0]
                case _:
                    raise DecodeError(reader.tell())
            offset = read_with(reader, struct.Struct("<I"))[0]
            key_mapping[value] = offset
        return cls(key_mapping=key_mapping, meta_header=meta_header)


class LeafNode:
    header: None

    ...
