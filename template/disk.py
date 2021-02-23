from template.config import *
from template.page import *
from template.pageRange import *
from template.pageSet import *


class Disk:
    def __init__(self, table_name):
        self.fn = table_name
        self.num_columns = 0
        self.next_base_rid = START_RID
        self.next_tail_rid = START_RID
        self.base_fn = TABLE_BASE_PATH + table_name + ".base"  # holds base records
        self.tail_fn = TABLE_BASE_PATH + table_name + ".tail"  # holds tail records
        self.info_fn = TABLE_BASE_PATH + table_name + ".info"  # holds table info
        self.base_meta_fn = TABLE_BASE_PATH + table_name + ".bmeta"  # uses base rids to indicate the start block of a page ranges and base page sets meta data
        self.tail_meta_fn = TABLE_BASE_PATH + table_name + ".tmeta"  # uses tail rids to indicate the start block of a tail page sets meta data
        self.page_directory_fn = TABLE_BASE_PATH + table_name + ".pd"  # uses  rids for indexing page ranges and base page sets internally

    # returns base page set with associated tail page sets
    def read(self, base_rid):
        pass

    def write(self, base_rid):
        pass

    def read_base_page(self, page_range_index, base_page_set_index, base_page_index):
        page = Page()
        file_offset = ((page_range_index * 16 * self.num_columns) + (base_page_set_index * self.num_columns) + base_page_index) * PAGE_SIZE
        f = open(self.base_fn, "rb")
        f.seek(file_offset)
        page.data = f.read(PAGE_SIZE)
        f.close()
        return page

    def read_tail_page(self, tail_page_set_index, base_page_index):
        pass

    def read_base_page_set(self, page_range_index, base_page_set_index):
        page_set = PageSet(self.num_columns)
        file_offset = ((page_range_index * 16 * self.num_columns) + (base_page_set_index * self.num_columns)) * PAGE_SIZE
        f = open(self.base_fn, "rb")
        f.seek(file_offset)
        for i in range(self.num_columns):
            page_set.pages[i].data = f.read(PAGE_SIZE)
        f.close()
        return page_set

    def read_tail_page_set(self, tail_page_set_index):
        pass

    def read_page_range(self, page_range_index):
        page_sets = []
        file_offset = (page_range_index * 16 * self.num_columns) * PAGE_SIZE
        f = open(self.base_fn, "rb")
        f.seek(file_offset)
        for i in range(16):
            page_set = PageSet(self.num_columns)
            for j in range(self.num_columns):
                page_set.pages[j].data = f.read(PAGE_SIZE)
            page_sets.append(page_set)
        f.close()
        return page_sets

    def read_table_info(self):
        f = open(self.info_fn, "rb")
        self.num_columns = int.from_bytes(f.read(8), "big")
        self.next_base_rid = int.from_bytes(f.read(8), "big")
        self.next_tail_rid = int.from_bytes(f.read(8), "big")
        f.close()

    def read_bmeta(self, base_rid):
        pass

    def read_tmeta(self, tail_rid):
        pass

    def read_base_page_directory(self, base_rid):
        pass

    def read_tail_page_directory(self, tail_rid):
        pass
