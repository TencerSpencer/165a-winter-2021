from template.table import Table

class Database():

    def __init__(self):
        self.tables = {}
        pass

    def open(self, path):
        pass

    def close(self):
        # when close is called, we must shutdown the timer for each table
        #tables = self.__get_tables_for_close()
        tables = self.tables.values()
        for table in tables:
            table.shut_down_timer()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        self.tables[name] = table
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
        return self.tables.get(name)


    """
    # get all tables in array form
    """
    def __get_tables_for_close(self):
        test = self.tables.values()
        keys = self.tables.keys()
        tables = []
        for i in range(keys):
            tables.append(self.tables.get(i))
        return tables
