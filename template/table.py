from template.index import Index
from template.pageRange import PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


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
        self.key = key # This is the index of the table key in columns that are sent in
        self.keys = {}  # key-value pairs { record key : (rid, page range index, base page set index) }
        self.num_columns = num_columns
        self.page_directory = {} # are keys and page_directory the same thing?
        self.index = Index(self)
        self.available_rid = 0
        
        # adding an array of page ranges here
        self.page_range_array = []

        pass

    def __merge(self):
        pass

    def insert_record(self, *columns):
        column_to_read = self.key 
        current_rid = self.__get_next_rid()
        # treat *columns as a list
        col_list = list[columns]
        key_col = col_list[self.key]

        next_free_page_range_index = __get_next_available_page_range()
        next_free_base_bage_set_index = self.page_range_array(next_free_page_range_index).get_next_free_base_page_set()
        
        # insert dictionary entry
        self.__add_dict_record(key_col, current_rid, next_free_page_range_index, next_free_base_bage_set_index)

        col_list.remove(self.key) # remove key entry

        # continue with inserting the record here
        curr_page_range = self.page_range_array(next_free_page_range_index)
        curr_page_range.write_base_record(current_rid, col_list)


    def __add_dict_record(self, key, new_rid, page_range_index, base_page_set_index):
        # need to find page range index, so next free page range, and next free base page set index
        keys[key] = (new_rid, page_range_index, base_page_set_index)


    def update_record(self):
        pass

    # todo, handler from query to table for select
    def select_record(self):
        pass 



    # page set function will be within a page range, not here
    
    # build new RID
    def __get_next_rid(self):
        current_rid = self.available_rid
        self.available_rid = self.available_rid + 1
        return current_rid

    # returns an index of the next available page range
    def __get_next_available_page_range(self):
        if not page_range_array:
            # no current page range, build one for starters and append to list
            new_page_range = PageRange(self.num_columns)
            self.page_range_array.append(new_page_range)
            # our index will be our only page range
            return 0

         # note, I assume that base records will be removed at some point, so I choose to loop through each
        for i in range (len(self.page_range_array)):
            if not self.page_range_array[i].is_full():
                return i

        # if we reeached here, then all our current page ranges are full. As such, build a new one
        new_page_range = PageRange(self.num_columns)
        self.page_range_array.append(new_page_range)
        
        # length returns an index + 1, so subtract one to compensate
        return len(self.page_range_array-1)


    def __get_page_range_from_key(self, key):
        return keys[key][0]  

    def __get_page_range_from_key(self, key):
        return keys[key][1]
        pass
    
    # may not be necessary
    def __get_page_set_from_key(self, key):
        return keys[key][2]
        pass

    



