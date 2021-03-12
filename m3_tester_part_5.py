from template.db import Database
from template.query import Query
from template.transaction import Transaction
from template.transaction_worker import TransactionWorker
from template.config import init
from template.lock_manager_config import *
from random import choice, randint, sample, seed

# TESTING UPDATE ABORT
init()
db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
seed(3562901)
num_threads = 4

try:
    grades_table.index.create_index(1)
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

transaction_workers = []
insert_transactions = []
select_transactions = []
update_transactions = []
for i in range(num_threads):
    insert_transactions.append(Transaction())
    select_transactions.append(Transaction())
    update_transactions.append(Transaction())
    transaction_workers.append(TransactionWorker())
    transaction_workers[i].add_transaction(insert_transactions[i])
    #transaction_workers[i].add_transaction(select_transactions[i])
    #transaction_workers[i].add_transaction(update_transactions[i])
worker_keys = [ {} for t in transaction_workers ]

'''
for i in range(0, 10000):
    key = 92106429 + i
    keys.append(key)
    i = i % num_threads
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    q = Query(grades_table)
    insert_transactions[i].add_query(q.insert, q.table, *records[key])
    worker_keys[i][key] = True
'''

q = Query(grades_table)
for i in range(0, 100):
    key = 92106429 + i
    keys.append(key)
    i = i % num_threads
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]

# test to see if the same key can be inserted twice

for i in range(0, 5):
    insert_transactions[0].add_query(q.insert, q.table, *records[keys[i]])

for i in range(6, 10):
    insert_transactions[0].add_query(q.insert, q.table, *records[keys[i]])
    

for i in range(10, 20):
    insert_transactions[1].add_query(q.insert, q.table, *records[keys[i]])
    

for i in range(20, 30):
    insert_transactions[2].add_query(q.insert, q.table, *records[keys[i]])
    pass


for i in range(20, 30):
    test_record = [None, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    insert_transactions[2].add_query(q.update, q.table, keys[i], *test_record)
    pass


temp_record = [1,1,1,1,1]
temp_key = -1
insert_transactions[2].add_query(q.select, q.table, temp_key, 0, temp_record)

for i in range(30, 40):
    insert_transactions[3].add_query(q.insert, q.table, *records[keys[i]])
    pass


for transaction_worker in transaction_workers:
    transaction_worker.run()

db.close()
db = Database()
db.open('./ECS165')
grades_table = db.get_table('Grades')

# select records between 20 and 24 to ensure they are rolled back
q = Query(grades_table)

#print("testing insertion rollback")
for i in range(20, 25):
    result = q.select(keys[i], 0, [1, 1, 1, 1, 1])
    if result:
        print("record was rolled back nice :)")
    else:
        print("record not rolled back ;(")

db.close()

