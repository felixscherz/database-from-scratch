# Building a database from scratch

## Data types

Define a set of data types that the database supports. These will have to be handled by the storage engine which
needs to seralize/deserialize them when writing to or reading from the disk.


## Storage engine

Data is stored in pages managed by the storage engine.
Clients can insert a row by passing the data and associated key.


### API

```python
def insert(key, data) -> None:
    ...

def read(key, data):
    ...
```

### Pages

Pages have a fixed size (commonly 4KB) and start with a header to contains information on the page type, number of
items, free space and offsets to other parts of the page.
