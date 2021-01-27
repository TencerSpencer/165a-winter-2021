from template.pageSet import PageSet
from template.config import *


class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_page_sets = []
        self.tail_page_sets = []
        self.init_base_page_sets()

    def init_base_page_sets(self):
        for i in range(PAGE_SETS):
            self.base_page_sets.append(PageSet(self.num_columns))

    def add_tail_page_set(self):
        self.tail_page_sets.append(PageSet(self.num_columns))

    def add_record(self, *columns):
        pass

    def remove_record(self):
        pass

    def update_record(self):
        pass
