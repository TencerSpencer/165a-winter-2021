import threading
from template.query import Query
from template.config import *
from template.lock_manager_config import *
import queue

INSERT_TYPE = Query.insert
UPDATE_TYPE = Query.update
SELECT_TYPE = Query.select
DELETE_TYPE = Query.delete

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.completed_queries = []

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        switcher = {
            "insert": INSERT_TYPE,
            "update": UPDATE_TYPE,
            "select": SELECT_TYPE,
            "delete": DELETE_TYPE
        }

        self.queries.append((query, table, args, switcher.get(query.__name__)))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        result = None
        for query, table, args, query_type, in self.queries:
            if query_type == SELECT_TYPE:
                result = query(args[0], args[1], *args[2:])
            elif query_type == UPDATE_TYPE:
                result = query(args[0], *args[1:])
            elif query_type == INSERT_TYPE:
                result = query(*args)
            elif query_type == DELETE_TYPE:
                result = query(*args)
            if result is False:
                return self.abort()
            self.completed_queries.append((query, table, args, query_type))
        return self.commit()

    def abort(self):

        # revert all changes made, no need to do anything for selection
        for query, table, args, query_type in self.completed_queries:
            if query_type == UPDATE_TYPE:           
                
                # get key for update
                key = args[0]
                table.roll_back_tail_with_key(key)

            elif query_type == INSERT_TYPE:

                # call removal with key
                table.remove_record(args[0])

            elif query_type == DELETE_TYPE:
                # insert removed information
                # this wont work because it's only the key
                # instead, revert the indirection type using the key instead of calling insert_record
                table.insert_record(*args)

        # remove locks
        LOCK_MANAGER.shrink(threading.currentThread().name)
        return False

    def commit(self):
        
        # remove locks
        LOCK_MANAGER.shrink(threading.currentThread().name)
        return True