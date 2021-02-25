import sys
sys.path.append('./')
import unittest
from template.db import *
from template.query import *

class DiskTests(unittest.TestCase):
    def test_create_new_table(self):
        db = Database()
        db.open("./ECS165")
        db.create_table("Test", 5, 0)
        db.close()

    def test_get_existing_table(self):
        db = Database()
        db.open("./ECS165")
        table = db.get_table("Test")
        db.close()

    def test_write_to_test_table(self):
        db = Database()
        db.open("./ECS165")
        table = db.get_table("Test")
        query = Query(table)
        for i in range(100):
            query.insert(*[i, 1, 2, 3, 4])
        db.close()

    # select(key, column, query_columns)
    def test_read_from_test_table(self):
        db = Database()
        db.open("./ECS165")
        table = db.get_table("Test")
        query = Query(table)
        for i in range(100):
            query.select(i, -1, [None, 1, None, None, None])
        db.close()


if __name__ == '__main__':
    unittest.main()
