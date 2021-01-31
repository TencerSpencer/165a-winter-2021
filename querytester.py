from template.db import Database
from template.query import Query
# from template.config import init

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

query.insert(*[0, 3, 2, 3, 4, 5])
query.insert(*[420, 69, 7, 8, 9, 10])
# query.increment(420, 0)
print(query.select(0, 0, [1,None, None, None, None]))
print(query.select(420, 0, [1,None,None,None,None]))

