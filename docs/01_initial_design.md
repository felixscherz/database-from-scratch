# Initial design

## Datatypes

Define a set of datatypes that the database is supposed to support. Then move on to implement a storage
engine that can store/retrieve those datatypes.
A piece of data will initially be represented as a tuple of those datatypes together with an associated key.

##### Supported data types
- `int`
- `float`
- `str`
- `bytes`

We will have to think about container types like `list` and `dict` in the future.

## API

The storage engine should implement the following initially:

```python
DBType: TypeAlias = int | float | str | bytes


def insert(key: DBType, value: tuple[DBType, ...]) -> None:
    ...

def read(key: DBType) -> tuple[DBType, ...]:
    ...
```

## Storage engine

The storage engine will store data inside pages of a fixed size. Each page starts with a header that contains
information on the type of the page, number of rows, free space and then a region of key-to-offset mappings.


### Page layout

1. Header region of fixed size
2. Key -> Offset mapping in sorted order
3. free space
4. Actual data is inserted from the back in insertion order

![Hi](../images/page_layout.png)


### How to organize pages

When a query for a certain key comes in, how do we know which page to look at to find the item without scanning the
entire database? We have to come up with a data structure that let's us find items by their key.

Our first design will use a B+ Tree. A B+ Tree has two types of nodes, internal nodes that hold a set of keys pointing
to child nodes and leaf nodes that hold the actual data.

When a new item is written, we start off at the root node and traverse the tree by comparing the insertion key with
existing keys. Once we hit a leaf node with empty space, we insert the item at the correct position.
If the leaf node happens to be full, we split it up into two new leaf nodes and create new entries for each leaf node
in the parent node.


### What about rows that exceed the page size?

After some research, this is how databases commonly handle rows that exceed page size.
The technique is called TOAST and works by storing a row that only contains references to the actual data in the main table. The actual data is saved inside a special TOAST table where the data is chunked into smaller sizes (usually 2KB) and stored in a chunk per row. 
On read, the database has to resolve the references and re-assmble the chunked data in the TOAST table.

We are not going to worry about this for now however.
