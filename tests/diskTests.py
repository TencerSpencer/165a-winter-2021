import unittest
from template.db import *

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


if __name__ == '__main__':
    unittest.main()
