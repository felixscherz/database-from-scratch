# Getting started

First we need to define a couple more things. We want to store data organized in tables, so our storage engine
should take that into account. This evolves the API a little bit:

```python
DBType: TypeAlias = int | float | str | bytes


def insert(table: str, key: DBType, value: tuple[DBType, ...]) -> None:
    ...

def read(table, key: DBType) -> tuple[DBType, ...]:
    ...
```

A naive approach could be to create a file for every table using the table name. When writing to the
table we would add pages to that file. In the future we will have to think about what happens if a table
grows very large and how to split it up into partitions in that case.

## Should we define a schema for a table? What are the benefits?

A benefit of working with a defined schema is that we know the size we need to allocate for an item ahead of time.
This makes it easier to allocate space for new items and figure out how many we can fit on a page.

To make things easier on us, we are going to go with a fixed schema per table. This reduces the amount of ege
cases we need to consider when implementing the storage engine.

## Table API

Before we can think about the API to read and write we need to think about how to create tables.
Reads and writes can only be implemented once we have tables.

The key should be part of the schema, indicated separately.

```python
Schema: TypeAlias = dict[str, type[DBType]]

def create_table(name: str, schema: Schema, primary_key: str | Sequence[str]):
    """primary_key must be present in schema"""
    ...

```

## How are we going to deal with composite primary keys?
