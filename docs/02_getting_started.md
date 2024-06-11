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
