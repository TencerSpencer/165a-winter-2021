from template.pageSet import PageSet
from template.config import *
import time


class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.num_records = 0
        self.base_page_sets = []
        self.tail_page_sets = []
        self.rids = {}  # maps rids to (base_page_set index, page offset)
        self.schema_encodings = []
        self.indirections = []
        self.timestamps = []
        self.__init_base_page_sets()

    def __init_base_page_sets(self):
        for i in range(PAGE_SETS):
            self.base_page_sets.append(PageSet(self.num_columns))

    def __add_tail_page_set(self):
        self.tail_page_sets.append(PageSet(self.num_columns))

    def add_record(self, *columns):
        # add key-value pair to rids where key is the rid and the value is a tuple containing the
        # base_page_set index and record offset
        self.rids[START_RID + self.num_records] = (self.num_columns / RECORDS_PER_PAGE, self.num_records)

        # add record to page set
        self.base_page_sets[self.num_records / RECORDS_PER_PAGE].add_record(columns)

        # add appropriate schema encoding, indirection, and timestamp
        self.schema_encodings[self.num_records] = '0' * self.num_columns
        self.indirections[self.num_records] = None
        self.timestamps[self.num_records] = time.strftime("%H:%M", time.localtime())

        self.num_records += 1

    def get_record(self, rid, query_columns):
        page_set_index, offset = self.rids[rid]
        return self.base_page_sets[page_set_index].read_record(offset, query_columns)

    def remove_record(self):
        pass

    def update_record(self):
        pass
