from template.lock_manager_config import LOCK_MANAGER
from template.table import Table, Record
from template.index import Index
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions=None):
        if transactions is None:
            transactions = []
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.local_page_sets
        self.worker_thread = threading.Thread(target = self.__execute_transactions)
        LOCK_MANAGER.add_to_thread_list(self.worker_thread)
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs a transaction
    """
    def run(self):
        self.worker_thread.start()
        pass


    def __execute_transactions(self):
        for transaction in self.transactions:
          #  each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))