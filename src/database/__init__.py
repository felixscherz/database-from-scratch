
class Database:
    def create_table(self, name: str, schema, primary_key):
        ...


    def insert(self, item):
        # need to figure out if there is a page with empty space where this fits
        # if yes, then insert it into that page
        # if no, create a new page and insert it there
        ...

    def read(self, key):
        ...
