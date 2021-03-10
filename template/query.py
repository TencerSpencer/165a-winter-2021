from template.table import Table, Record
from template.index import Index
from template.lock_manager_config import *
import copy


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, key):
        # Remove all entries from built indices
        rid, columns = self.table.select_record(key, [1]*self.table.num_columns)
        if self.table.index != None:
            for i in range(self.table.num_columns):
                if self.table.index.is_index_built(i):
                    self.table.index.delete(i, columns[i], rid)
        # Remove the record from the table
        return self.table.remove_record(key)

    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        status, rid = self.table.insert_record(*columns)
        if status and self.table.index != None:
            for i in range(len(columns)):
                if self.table.index.is_index_built(i):
                    self.table.index.insert_into_index(i, columns[i], rid)
        else:
            return status

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, key, column, query_columns):
        data = []
        if column != self.table.key and self.table.index != None and self.table.index.is_index_built(column):
            rids = self.table.index.locate(column, key)
            for rid in rids:
                record = self.table.select_record_using_rid(rid, query_columns)
                if record:
                    data.append(Record(record[0], record[1][self.table.key], record[1]))
                else:
                    return False
        elif column != self.table.key:
            # iterate over selections to see if the selected col value in column column == key
            keys = self.table.safe_get_keys()
            for globalKey in keys:
                record = self.table.select_record(globalKey, [1]*self.table.num_columns)
                if not record:
                    return False
                if record[1][column] == key:
                    # Found a match, add it to data
                    data.append(Record(record[0], globalKey, self.__trim(record[1], query_columns)))
           # LOCK_MANAGER.latches[PAGE_DIR].release()
        else:
            data = self.table.select_record(key, query_columns)
            if data:
                record = [Record(data[0], key, data[1])]
                return record
        if data == False or len(data) == 0:
            return False
        return data

    def __trim(self, to_trim, query_columns):
        output = []
        for i in range(len(to_trim)):
            if query_columns[i] != None:
                output.append(to_trim[i])
            else:
                output.append(None)
        return output

    """ INCLUSIVE!!!"""
    def select_range(self, begin, end, column, query_columns):
        data = []
        if self.table.index != None and self.table.index.is_index_built(column):
            RIDs = self.table.index.locate_range(column, begin, end)
            for rid in RIDs:
                raw = self.table.select_record_using_rid(rid, query_columns)
                record = Record(raw[0], raw[1][self.table.key], raw[1])
                data.append(record)
        else:
            # Need to brute force it
            for globalKey in self.table.keys.keys():
                record = self.table.select_record(globalKey, [1] * self.table.num_columns)
                if record[1][column] >= begin and record[1][column] <= end:
                    # Found a match, add it to data
                    data.append(Record(record[0], globalKey, record[1]))
        return data

    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, key, *new_columns):
        LOCK_MANAGER.latches[KEY_DICT].acquire()
        rid = self.table.keys.get(key, None)
        LOCK_MANAGER.latches[KEY_DICT].release()
        if rid == None:
            return False
        # Update the indices, if built
        old_record = self.table.select_record(key, [1] * self.table.num_columns)[1]
        if self.table.index != None:
            for i in range(len(new_columns)):
                value = new_columns[i]
                if value == None:
                    continue
                if self.table.index.is_index_built(i):
                    self.table.index.update_value(i, old_record[i], value, rid)
        # if new_columns overwrites key:
        ret = self.table.update_record(key, *new_columns)
        if new_columns[self.table.key] != None:
            LOCK_MANAGER.latches[KEY_DICT].acquire()
            rid = self.table.keys[key]
            # Remove mapping from key to rid
            del self.table.keys[key]
            # Add the new mapping
            self.table.keys[new_columns[self.table.key]] = rid
            LOCK_MANAGER.latches[KEY_DICT].release()
        return ret

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        """if self.table.index != None and self.table.index.is_index_built(self.table.key):
            return self.table.index.get_sum(self.table.key, start_range, end_range)"""
        sum = 0
        query_cols = [None] * self.table.num_columns
        query_cols[aggregate_column_index] = 1
        run = False
        for i in range(start_range, end_range + 1):
            result = self.select(i, 0, query_cols)
            # print(result)
            if result and result[0]:
                sum += result[0].columns[aggregate_column_index]
                run = True
        if not run:
            return False
        return sum

    """
    increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        # print(self.select(key, self.table.key, [1] * self.table.num_columns))
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            r = r.columns
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False