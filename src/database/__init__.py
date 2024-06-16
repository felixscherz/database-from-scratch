import struct
from io import BytesIO
from typing import Any
from typing import BinaryIO
from typing import NamedTuple
from typing import Sequence
from typing import TypeAlias

from database.nodes import MetaHeader

Datatype: TypeAlias = str | int | float | bytes

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


class DecodeError(Exception):
    def __init__(self, offset: int):
        super().__init__(f"Invalid bytes encountered at {offset=}")


class SchemaError(Exception):
    def __init__(self, value: Any, pos: int, expected: type):
        super().__init__(f"Value {value!r} at position {pos} does not match the schema. Expected {expected!r}")


def read_with(reader: BinaryIO, struct: struct.Struct):
    return struct.unpack(reader.read(struct.size))


class TableMetadata(NamedTuple):
    schema: dict[str, type[Datatype]]
    key_sequence: tuple[str, ...]


class Database:
    def __init__(self):
        self.b = BytesIO()

    def create_table(self, name: str, schema: dict[str, type[Datatype]], primary_key: str | Sequence[str]):
        # i want a dictionary of name to index
        key_sequence = (primary_key,) if isinstance(primary_key, str) else tuple(primary_key)
        meta_header = MetaHeader(
            magic=MAGIC_BYTES,
            version=VERSION_BYTES,
            bytes_per_page=BYTES_PER_PAGE,
            number_of_pages=1,
            schema=schema,
            primary_key=key_sequence
        )
        meta_header.write(self.b)


        b = self.b

        # start with a leaf page for an empty table
        b.write(TYPE_OF_PAGE_STRUCT.pack(LEAF_NODE))
        b.write(struct.pack("<I", 0))  # number of items on the page
        b.write(
            struct.pack("<I", BYTES_PER_PAGE - b.tell() - 4 * struct.calcsize("<I"))
        )  # free space left on the page after accounting for other headers
        b.write(struct.pack("<I", BYTES_PER_PAGE))  # point to end of the first page
        b.write(struct.pack("<I", b.tell() + 2 * struct.calcsize("<I")))  # point to start of free space
        b.write(struct.pack("<I", BYTES_PER_PAGE))  # point to first item in page

    def insert(self, table_name: str, item: tuple[Datatype, ...]):
        # when calling this, we might know nothing about the table, so we first need to parse the metaheader for
        # information on the schema and where to begin traversing the b+ tree
        schema = self.schema(table_name)
        for i, datatype in enumerate(schema.values()):
            if not isinstance(item[i], datatype):
                raise SchemaError(item[i], i, datatype)
        # start at root node
        # if root node is empty, add the value
        # if root node has values but is not full -> add item
        # if root node is more than 75% full

    def read(self, key): ...

    def schema(self, name: str):
        return self._parse_meta_header(self.b).schema

    def primary_key(self, name: str):
        return self._parse_meta_header(self.b).key_sequence

    def _parse_meta_header(self, reader: BinaryIO):
        reader.seek(0)
        if not read_with(reader, FILE_HEADER_MAGIC) == MAGIC_BYTES:
            raise DecodeError(0)
        version = read_with(reader, VERSION_BYTES_STRUCT)
        bytes_per_page = read_with(reader, BYTES_PER_PAGE_STRUCT)
        number_of_pages = read_with(reader, NUMBER_OF_PAGES_STRUCT)
        number_of_columns = read_with(reader, NUMBER_OF_COLUMNS_STRUCT)
        schema = {}
        key_mapping = {}
        for _ in range(number_of_columns[0]):
            name_length = read_with(reader, struct.Struct("<b"))
            name = read_with(reader, struct.Struct(f"<{name_length[0]}s"))
            is_primary_key = read_with(reader, struct.Struct("<?"))
            if is_primary_key[0]:
                key_index = read_with(reader, struct.Struct("<B"))
                key_mapping[key_index[0]] = name[0].decode()
            datatype = read_with(reader, DATATYPE_STRUCT)
            schema[name[0].decode()] = BYTE_TO_DATATYPE[datatype[0]]
        key_sequence = tuple(key_mapping[i] for i in range(len(key_mapping)))

        return TableMetadata(schema, key_sequence)
