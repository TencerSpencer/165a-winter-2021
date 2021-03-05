from template.table import Table, Record
from template.query import Query
from template.index import Index

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
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        switcher = {
            "insert": INSERT_TYPE,
            "update": UPDATE_TYPE,
            "select": SELECT_TYPE,
            "delete": DELETE_TYPE
        }

        self.queries.append((query, args, switcher.get(query.__name__)))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args, query_type in self.queries:
            if query_type == SELECT_TYPE:
                pass
            elif query_type == UPDATE_TYPE or query_type == INSERT_TYPE:
                pass
            elif query_type == DELETE_TYPE:
                pass
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    def commit(self):
        # TODO: commit to database
        return True