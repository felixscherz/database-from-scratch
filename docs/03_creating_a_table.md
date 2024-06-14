# Creating a table


## File layout

```
# meta header, only part of the first page
0 | magic bytes
1 | version
2 | size of a page
3 | number of pages
4 | number of columns in schema
------ for every column -----
1 | length of name
2 | name
3 | is primary key flag
4 | datatype byte
-----------------------------

```
```
# internal node
0 | type of page -> internal
1 | number of items on page
2 | amount of free space left
3 | offset to end of the page
4 | offset to start of free space
5 | offset to start of first item on the page
--------- for every key -------------
1 | mapping from key to page number for the next node
-------------------------------------
1 | <free space>
```

```
# leaf node
0 | type of page -> leaf
1 | number of items on page
2 | amount of free space left
3 | offset to end of the page
4 | offset to start of free space
5 | offset to start of first item on the page
6 | mapping from key to page internal offset
7 | <free space>
8 | items in insertion order

```

## Encoding the schema

We can assign every primitive type a byte and then encode rows as a byte sequence. This does not account for complex
types. To support complex types we define bytes for every container type which indicate that what follows are primitive
types that are part of the container. Let's only deal with primitive types for now.

## Removing the checksum part

We are going to make it easier on us to implement an skip worrying about a checksum for now.
