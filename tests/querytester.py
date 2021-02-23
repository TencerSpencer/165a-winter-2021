from template.db import Database
from template.query import Query
from template.index import Index
import time

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
print("Inserting 1M items")
for i in range(1_000_000):
    query.insert(*[i, i, i+1, i+2, i+3])
# query.insert(*[1, 2, 3, 4, 5])

# query.update(42, *[11,12,13,14,15])
# query.increment(420, 0)
#print(query.select(0, 0, [1,None, None, None, None]))
#print(query.select(42, 0, [1,None,None,None,None]))
#print(query.sum(0, 50, 0))
query.delete(42)
#print(query.select(42, 0, [1,None,None,None,None]))
firstTime = time.perf_counter()
print('--- Index Testing ---')
index = Index(grades_table)
index.create_index(1)
print("Took " + str(time.perf_counter() - firstTime) + " to build index")

