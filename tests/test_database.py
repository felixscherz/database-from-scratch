from database import Database


def test_read_after_write():
    db = Database()
    schema = {"id": int, "greeting": str}
    db.create_table("greetings", schema=schema, primary_key="id")
    db.insert("greetings", (0, "Hello World"))

    assert db.read(0) == ("Hello World")


def test_read_schema_for_existing_table():
    db = Database()
    schema = {"id": int, "greeting": str}
    db.create_table("greetings", schema=schema, primary_key="id")
    assert schema == db.schema("greetings")


def test_read_primary_key_for_existing_table():
    db = Database()
    schema = {"id": int, "greeting": str}
    db.create_table("greetings", schema=schema, primary_key="id")
    assert ("id",) == db.primary_key("greetings")


def test_read_primary_key_sequence_for_existing_table():
    db = Database()
    schema = {"id": int, "greeting": str, "info": str}
    db.create_table("greetings", schema=schema, primary_key=("id", "info"))
    assert ("id", "info") == db.primary_key("greetings")
