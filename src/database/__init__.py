import struct
from io import BytesIO
from typing import Any, BinaryIO
from typing import NamedTuple

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


def read_with(reader: BinaryIO, struct: struct.Struct):
    return struct.unpack(reader.read(struct.size))


class TableMetadata(NamedTuple):
    schema: dict[str, Any]


class Database:
    def __init__(self):
        self.b = BytesIO()

    def create_table(self, name: str, schema, primary_key):
        b = self.b
        b.write(FILE_HEADER_MAGIC.pack(*MAGIC_BYTES))
        b.write(VERSION_BYTES_STRUCT.pack(*VERSION_BYTES))
        b.write(BYTES_PER_PAGE_STRUCT.pack(BYTES_PER_PAGE))
        b.write(NUMBER_OF_PAGES_STRUCT.pack(1))
        b.write(NUMBER_OF_COLUMNS_STRUCT.pack(len(schema)))
        for name, datatype in schema.items():
            # <length-of-name><name><is-primary-key><datatype>
            b.write(struct.pack("<b", len(name)))
            b.write(struct.pack(f"<{len(name)}s", name.encode()))
            # this could be changed to encode the key position 0,1,2,3 to allow for composite keys
            b.write(struct.pack("<?", name == primary_key))
            b.write(DATATYPE_STRUCT.pack(DATATYPE_TO_BYTE[datatype]))

        # start with a leaf page for an empty table
        b.write(TYPE_OF_PAGE_STRUCT.pack(LEAF_NODE))
        b.write(struct.pack("<I", 0))  # number of items on the page
        b.write(
            struct.pack("<I", BYTES_PER_PAGE - b.tell() - 4 * struct.calcsize("<I"))
        )  # free space left on the page after accounting for other headers
        b.write(struct.pack("<I", BYTES_PER_PAGE))  # point to end of the first page
        b.write(struct.pack("<I", b.tell() + 2 * struct.calcsize("<I")))  # point to start of free space
        b.write(struct.pack("<I", BYTES_PER_PAGE))  # point to first item in page

    def insert(self, item):
        # when calling this, we might know nothing about the table, so we first need to parse the metaheader for
        # information on the schema and where to begin traversing the b+ tree
        ...

    def read(self, key): ...

    def table_info(self, name: str):
        return self._parse_meta_header(self.b)

    def _parse_meta_header(self, reader: BinaryIO):
        reader.seek(0)
        if not read_with(reader, FILE_HEADER_MAGIC) == MAGIC_BYTES:
            raise DecodeError(0)
        version = read_with(reader, VERSION_BYTES_STRUCT)
        bytes_per_page = read_with(reader, BYTES_PER_PAGE_STRUCT)
        number_of_pages = read_with(reader, NUMBER_OF_PAGES_STRUCT)
        number_of_columns = read_with(reader, NUMBER_OF_COLUMNS_STRUCT)
        schema = {}
        for _ in range(number_of_columns[0]):
            name_length = read_with(reader, struct.Struct("<b"))
            name = read_with(reader, struct.Struct(f"<{name_length[0]}s"))
            is_primary_key = read_with(reader, struct.Struct("<?"))
            datatype = read_with(reader, DATATYPE_STRUCT)
            schema[name[0].decode()] = BYTE_TO_DATATYPE[datatype[0]]
        return TableMetadata(schema)
