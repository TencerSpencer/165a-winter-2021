
from template.db import Database
from template.query import Query
from template.index import *
import time

db = Database()
db.open("./ECS165")
grades_table = db.create_table('QueryTester', 5, 0)

#index = Index(grades_table)

query = Query(grades_table)
# print("Inserting 1M items")
for i in range(1_001):
   query.insert(*[i, i, i+1, i+2, i+3])
for record in query.select_range(500, 525, 0, [1,1,1,1,1]):
    print(record.columns)
#query.insert(*[1, 2, 3, 4, 5]) # <-
#query.insert(*[1, 3, 4, 5, 6])
#query.insert(*[2, 1, 3, 3, 4])
# query.select(2, 1, [None, None, None, None, 1])[0].columns
#index.drop_index(1)
#print(query.select(2, 1, [1, 1, 1, 1, 1])[0].columns)
#print(query.select(4, 2, [1, None, None, None, None])[0].columns[0])

"""print(query.delete(1))
print(query.select(2, 1, [None, None, None, None, 1]))
print(query.select(3, 1, [None, None, None, None, 1]))
print(query.select(2,0,[None, None, None, None, 1]))"""

query.insert(*[42, 42, 3, 4, 5])


query.insert(*[88, 7, 12, 18, 99])
# query.delete(42)
"""query.insert(*[88, 7, 12, 18, 99])
query.insert(*[88, 7, 12, 18, 99])
query.insert(*[88, 7, 12, 18, 99])"""

index = Index(grades_table)
index.create_index(1)
# query.update(42, *[11,12,13,14,15])
query.increment(42, 1)
# print(query.select(43, 1, [None,1, None, None, None])[0].columns)
#print(query.select(43, 1, [1, 1, None, None, None])[0].columns)
#query.delete(42)
#print(query.select(43, 1, [1, 1, None, None, None]))
#print(query.select(42, 0, [1, 1, None, None, None]))
#print(query.select(2, 1, [1,None,None,None,None]))
"""index = Index(grades_table)
index.is_index_built(1)
print(query.sum(0, 50, 0))
query.delete(42)
print(query.select(42, 0, [1,None,None,None,None]))"""

"""firstTime = time.perf_counter()
print('--- Index Testing ---')
index = Index(grades_table)
index.create_index(1)
print("Took " + str(time.perf_counter() - firstTime) + " to build index")"""

"""
print('\n---Seed Timing---\n')
print('SIZE 3') #SEEDS = 0, 500, 1000
firstTime = time.perf_counter()
index.locate()
firstTime = time.perf_counter()
query.select()
"""
db.close()
