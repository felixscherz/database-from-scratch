from database import Database


def test_read_after_write():
    db = Database()
    schema = {"id": int, "greeting": str}
    db.create_table("greetings", schema=schema, primary_key="id")
    db.insert((0, "Hello World"))

    assert db.read(0) == ("Hello World")
