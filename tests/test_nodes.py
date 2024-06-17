from io import BytesIO
from database.nodes import InternalNode
from database.nodes import MetaHeader


def test_internal_node_can_be_serialized():
    meta_header = MetaHeader(
        number_of_pages=1,
        schema={"id": str},
        primary_key=("id",),
    )
    node = InternalNode(key_mapping={}, meta_header=meta_header)

    b = BytesIO()
    node.write(b)
    b.seek(0)

    assert InternalNode.read(b, root=True) == node

