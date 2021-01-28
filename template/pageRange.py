from template.pageSet import PageSet
from template.config import *


class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.num_records = 0
        self.base_page_sets = []
        self.tail_page_sets = []
        self.next_rid = 0
        self.__init_base_page_sets()

    def __init_base_page_sets(self):
        for i in range(PAGE_SETS):
            self.base_page_sets.append(PageSet(self.num_columns))

    def __add_tail_page_set(self):
        self.tail_page_sets.append(PageSet(self.num_columns))

    def add_record(self, *columns):
        # add record to appropriate page set
        rid = self.create_rid()
        page_set_index = self.get_next_free_base_page_set()
        self.base_page_sets[page_set_index].write_base_record(rid, columns)
        self.num_records += 1
        return rid, page_set_index

    # setup is simplistic due to cumulative pages
    def get_record(self, rid, page_set_index, query_columns):

        # start at the base page, get its schema too
        base_page_to_read = self.base_page_sets[page_set_index]
        tail_page = base_page_to_read.get_indirection(rid)
        
        # get only the base page's info
        if tail_page == None:
            return get_record_only_base(self, rid, page_set_index, query_columns)
        
        else:

            read_data = []

            # obtain schema
            base_page_offset = base_page_to_read.rids[rid]
            base_page_schema = base_page_to_read.schema_encoding[base_page_offset]

            # filler function that must obtain a tail page handler from a given RID
            tail_page_to_read = get_tail_page()
            tail_page_rid = get_tail_page_rid()

            # pointer access isn't as expensive, utilize this to alternate page reads
            for i in range (self.num_columns):
                if (query_columns[i] != None):
                    if (base_page_schema >> i) == 1:
                        read_data.append(tail_page_to_read.read_record(tail_page_rid, query_columns, i))
                    else:
                        read_data.append(base_page_to_read.read_record(rid, query_columns, i))
            
            return read_data

                
    def get_record_only_base(self, rid, page_set_index, query_columns):
        
        read_data = []
        base_page_to_read = self.base_page_sets[page_set_index]
        for i in range (self.num_columns):
            read_data.append(base_page_to_read.read_record(rid, query_columns, i))
        
        return read_data


    def remove_record(self):
        # set given RID to null in directory
        pass

    def update_record(self, rid, page_set_index, *columns):
        
        # look for next available tail page, create one if it does not exist
        # get the next available RID and map it to a page in the given tail page
        # CUMULATIVE APPROACH: look at columns info and update it with the latest tail page's info
        # generate a new schema based on this UPDATED column and then have all this data get written to the tail page
        # set tail's schema and base's schema to the newly generated schema
        # if a previous tail existed, set that tail's indirection to the current tail
        # set the base's indirection to the new tail and the new tail's indirection to the base tail

        # I think that covers everything


        pass

    def create_rid(self):  # When we merge, do we continue to keep our pages?
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def get_next_free_base_page_set(self):
        return self.num_records / RECORDS_PER_PAGE

    def has_space(self):
        if self.num_records < PAGE_SETS * RECORDS_PER_PAGE:
            return True

        return False
        