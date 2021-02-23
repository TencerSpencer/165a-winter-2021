from template.config import *
from template.page import *
from template.pageRange import *
from template.pageSet import *


class Disk:
    def __init__(self, table_name):
        self.fn = table_name
        self.num_columns = None
        self.key_column = None
        self.next_base_rid = None
        self.next_tail_rid = None
        self.base_fn = TABLE_BASE_PATH + table_name + ".base"  # holds base records
        self.tail_fn = TABLE_BASE_PATH + table_name + ".tail"  # holds tail records
        self.info_fn = TABLE_BASE_PATH + table_name + ".info"  # holds table info
        self.base_meta_fn = TABLE_BASE_PATH + table_name + ".bmeta"  # uses base rids to indicate the start block of a page ranges and base page sets meta data
        self.tail_meta_fn = TABLE_BASE_PATH + table_name + ".tmeta"  # uses tail rids to indicate the start block of a tail page sets meta data
        self.keys_fn = TABLE_BASE_PATH + table_name + ".keys"  # uses entry as index
        self.brid_fn = TABLE_BASE_PATH + table_name + ".brid"  # uses entry as index
        self.trid_fn = TABLE_BASE_PATH + table_name + ".trid"  # uses entry as index
        self.base_page_directory_fn = TABLE_BASE_PATH + table_name + ".bpd"  # uses entry as index
        self.tail_page_directory_fn = TABLE_BASE_PATH + table_name + ".tpd"  # uses entry as index

    # returns base page set with associated tail page sets
    def read(self, base_rid):
        pass

    def write(self, base_rid):
        pass


    ### READ METHODS ###
    def read_keys(self):
        data = bytearray(0)
        with open(self.keys_fn, "rb") as f:
            while True:
                bytes = f.read(PAGE_SIZE)
                if not bytes:
                    break
                data.extend(bytes)

        keys = []
        for i in range(0, len(data), 8):
            keys.append(int.from_bytes(data[(i * 8):(i * 8) + 8], "little"))
        return keys

    def read_base_rids(self):
        data = bytearray(0)
        with open(self.brid_fn, "rb") as f:
            while True:
                bytes = f.read(PAGE_SIZE)
                if not bytes:
                    break
                data.extend(bytes)

        rids = []
        for i in range(0, len(data), 8):
            rids.append(int.from_bytes(data[(i * 8):(i * 8) + 8], "little"))
        return rids

    def read_tail_rids(self):
        data = bytearray(0)
        with open(self.trid_fn, "rb") as f:
            while True:
                bytes = f.read(PAGE_SIZE)
                if not bytes:
                    break
                data.extend(bytes)

        rids = []
        for i in range(0, len(data), 8):
            rids.append(int.from_bytes(data[(i * 8):(i * 8) + 8], "little"))
        return rids

    def read_base_pd(self, entry_index):
        f = open(self.base_page_directory_fn, "rb")
        f.seek(entry_index)
        data = f.read(8)
        return int.from_bytes(data, "little")

    def read_tail_pd(self, entry_index):
        f = open(self.tail_page_directory_fn, "rb")
        f.seek(entry_index)
        data = f.read(8)
        return int.from_bytes(data, "little")

    def read_base_page(self, page_range_index, base_page_set_index, base_page_index):
        page = Page()
        file_offset = ((page_range_index * 16 * self.num_columns) + (
                base_page_set_index * self.num_columns) + base_page_index) * PAGE_SIZE
        f = open(self.base_fn, "rb")
        f.seek(file_offset)
        page.data = f.read(PAGE_SIZE)
        f.close()
        return page

    def read_tail_page(self, tail_page_set_index, base_page_index):
        pass

    def read_base_page_set(self, page_range_index, base_page_set_index):
        page_set = PageSet(self.num_columns)
        file_offset = ((page_range_index * 16 * self.num_columns) + (
                base_page_set_index * self.num_columns)) * PAGE_SIZE
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
        self.num_columns = int.from_bytes(f.read(8), "little")
        self.key_column = int.from_bytes(f.read(8), "little")
        self.next_base_rid = int.from_bytes(f.read(8), "little")
        self.next_tail_rid = int.from_bytes(f.read(8), "little")
        f.close()


    ### WRITE METHODS ###
    def write_keys(self):
        data = bytearray(0)
        with open(self.keys_fn, "rb") as f:
            while True:
                bytes = f.read(PAGE_SIZE)
                if not bytes:
                    break
                data.extend(bytes)

        keys = []
        for i in range(0, len(data), 8):
            keys.append(int.from_bytes(data[(i * 8):(i * 8) + 8], "little"))
        return keys

    def write_base_rids(self):
        data = bytearray(0)
        with open(self.brid_fn, "rb") as f:
            while True:
                bytes = f.read(PAGE_SIZE)
                if not bytes:
                    break
                data.extend(bytes)

        rids = []
        for i in range(0, len(data), 8):
            rids.append(int.from_bytes(data[(i * 8):(i * 8) + 8], "little"))
        return rids

    def write_tail_rids(self):
        data = bytearray(0)
        with open(self.trid_fn, "rb") as f:
            while True:
                bytes = f.read(PAGE_SIZE)
                if not bytes:
                    break
                data.extend(bytes)

        rids = []
        for i in range(0, len(data), 8):
            rids.append(int.from_bytes(data[(i * 8):(i * 8) + 8], "little"))
        return rids

    def write_base_pd(self, entry_index):
        f = open(self.base_page_directory_fn, "rb")
        f.seek(entry_index)
        data = f.read(8)
        return int.from_bytes(data, "little")

    def write_tail_pd(self, entry_index):
        f = open(self.tail_page_directory_fn, "rb")
        f.seek(entry_index)
        data = f.read(8)
        return int.from_bytes(data, "little")

    def write_base_page(self, base_page, page_range_index, base_page_set_index, base_page_index):
        file_offset = ((page_range_index * 16 * self.num_columns) + (
                base_page_set_index * self.num_columns) + base_page_index) * PAGE_SIZE
        f = open(self.base_fn, "wb")
        f.seek(file_offset)
        f.write(base_page.data)
        f.close()

    def write_base_page_set(self, base_page_set, page_range_index, base_page_set_index):
        file_offset = ((page_range_index * 16 * self.num_columns) + (
                base_page_set_index * self.num_columns)) * PAGE_SIZE
        f = open(self.base_fn, "wb")
        f.seek(file_offset)
        for i in range(self.num_columns):
            f.write(base_page_set.pages[i].data)
        f.close()

    def write_page_range(self, base_page_sets, page_range_index):
        file_offset = (page_range_index * 16 * self.num_columns) * PAGE_SIZE
        f = open(self.base_fn, "wb")
        f.seek(file_offset)
        for i in range(16):
            page_set = base_page_sets[i]
            for j in range(self.num_columns):
                f.write(page_set.pages[j].data)
        f.close()

    def write_tail_page(self, tail_page_set_index, base_page_index):
        pass

    def write_tail_page_set(self, tail_page_set_index):
        pass
