from template.pageSet import PageSet
from template.config import *


class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.num_records = 0
        self.base_page_sets = []
        self.tail_page_sets = []
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
        self.base_page_sets[page_set_index].write_record(rid, columns)
        self.num_records += 1
        return rid, page_set_index

    def get_record(self, rid, page_set_index, query_columns):
        return self.base_page_sets[page_set_index].read_record(rid, query_columns)

    def remove_record(self):
        pass

    def update_record(self):
        pass

    def create_rid(self):
        return START_RID + self.num_records

    def get_next_free_base_page_set(self):
        return self.num_records / RECORDS_PER_PAGE

    def has_space(self):
        if self.num_records < PAGE_SETS * RECORDS_PER_PAGE:
            return True

        return False
