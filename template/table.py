from template.index import Index
from template.pageRange import PageRange
from template.config import *


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key  # This is the index of the table key in columns that are sent in
        self.num_columns = num_columns
        self.keys = {}  # key-value pairs { key : rid }
        self.brid_block_start = {}  # { base rid : block start index }
        self.trid_block_start = {}  # { tail rid : block start index
        self.page_directory = {}  # key-value pairs { rid : (page range index, base page set index) }
        self.index = Index(self)
        self.next_base_rid = START_RID
        self.next_tail_rid = START_RID
        self.page_range_array = []

    def __merge(self):
        pass

    def insert_record(self, *columns):
        key_col = self.key
        new_rid = self.__get_next_base_rid()
        col_list = list(columns)
        key = col_list[key_col]

        next_free_page_range_index = self.__get_next_available_page_range()
        next_free_base_page_set_index = self.page_range_array[next_free_page_range_index].get_next_free_base_page_set()

        # set key and rid mappings
        self.keys[key] = new_rid
        self.page_directory[new_rid] = (next_free_page_range_index, next_free_base_page_set_index)

        # continue with inserting the record here
        curr_page_range = self.page_range_array[next_free_page_range_index]
        return curr_page_range.add_record(new_rid, col_list)

    def update_record(self, key, *columns):
        base_rid = self.keys[key]
        page_range_index = self.page_directory[base_rid][0]
        tail_rid = self.__get_next_tail_rid()
        return self.page_range_array[page_range_index].update_record(base_rid, tail_rid, columns)

    def select_record(self, key, query_columns):
        if key not in self.keys:
            return False

        rid = self.keys[key]
        page_range_index = self.page_directory[rid][0]
        cur_page_range = self.page_range_array[page_range_index]
        data = cur_page_range.get_record(rid, query_columns)
        return rid, data

    def remove_record(self, key):
        if key in self.keys:
            self.keys.pop(key)
            return True

        return False

    def __get_next_base_rid(self):
        current_rid = self.next_base_rid
        self.next_base_rid += 1
        return current_rid

    def __get_next_tail_rid(self):
        current_rid = self.next_tail_rid
        self.next_tail_rid += 1
        return current_rid

    # returns an index of the next available page range
    def __get_next_available_page_range(self):
        if len(self.page_range_array) == 0:
            # no current page range, build one for starters and append to list
            new_page_range = PageRange(self.num_columns)
            self.page_range_array.append(new_page_range)
            # our index will be our only page range
            return 0

        # note, I assume that base records will be removed at some point, so I choose to loop through each
        for i in range(len(self.page_range_array)):
            if not self.page_range_array[i].is_full():
                return i

        # if we reached here, then all our current page ranges are full. As such, build a new one
        new_page_range = PageRange(self.num_columns)
        self.page_range_array.append(new_page_range)

        # length returns an index + 1, so subtract one to compensate
        return len(self.page_range_array) - 1
