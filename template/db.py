from template.table import Table
from template.disk import *

class Database():
    def __init__(self):
        self.tables = {}
        self.disks = {}
        self.path = ""
        self.buffer_pool = Bufferpool()


    def open(self, path):
        if os.path.isabs(path):
            self.path = path
        else:
            self.path = os.path.join(os.path.dirname(__file__), path)

        self.disks = Disk.get_all_disks(self.path)


    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = self.tables.get(name)
        if table:  # if table already exists
            return table

        d = Disk(self.path, name, num_columns, key)
        table = d.read_table()
        table.buffer_pool = self.buffer_pool
        table.disk = d
        self.disks[name] = d
        self.tables[name] = table
        table.buffer_pool.tables = self.tables
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables.keys():
            self.tables.pop(name)
        

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        disk = self.disks.get(name)
        table = self.tables.get(name)
        if table:  # if tables is already in self.tables
            return table

        if disk:  # if disk object associated with table exists, get table from disk
            table = disk.read_table()
            table.buffer_pool = self.buffer_pool
            table.disk = disk
            self.tables[name] = table
            table.buffer_pool.tables = self.tables
            return table

        return False


