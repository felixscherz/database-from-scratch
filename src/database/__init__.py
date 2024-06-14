import struct
from io import BytesIO

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

TYPE_OF_PAGE_STRUCT = struct.Struct("<b")
INTERNAL_NODE = 0
LEAF_NODE = 1


class Database:
    def create_table(self, name: str, schema, primary_key):
        b = BytesIO()
        b.write(FILE_HEADER_MAGIC.pack(*MAGIC_BYTES))
        b.write(VERSION_BYTES_STRUCT.pack(*VERSION_BYTES))
        b.write(BYTES_PER_PAGE_STRUCT.pack(BYTES_PER_PAGE))
        b.write(NUMBER_OF_PAGES_STRUCT.pack(1))
        b.write(NUMBER_OF_COLUMNS_STRUCT.pack(len(schema)))
        for name, datatype in schema.items():
            # <length-of-name><name><is-primary-key><datatype>
            b.write(struct.pack("<b", len(name)))
            b.write(struct.pack(f"<{len(name)}s", name.encode()))
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

        ...

    def read(self, key): ...
