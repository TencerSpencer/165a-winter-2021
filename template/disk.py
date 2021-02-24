from template.config import *
from template.page import *
from template.pageRange import *
from template.pageSet import *
import os
from template.table import *


class Disk:
    def __init__(self, db_dir, table_name, num_columns, key_column):
        self.fn = table_name
        self.db_dir = db_dir
        self.table_dir = os.path.join(db_dir, "Tables", table_name)
        self.base_fn = os.path.join(self.table_dir, table_name + ".base")
        self.tail_fn = os.path.join(self.table_dir, table_name + ".tail")
        self.info_fn = os.path.join(self.table_dir, table_name + ".info")
        self.key_directory = os.path.join(self.table_dir, table_name + ".kd")
        if not self.__files_exist():
            self.num_columns = num_columns
            self.key_column = key_column
            self.next_base_rid = START_RID
            self.next_tail_rid = START_RID
            self.__create_files(num_columns, key_column)
        else:
            self.__read_file_info()

    @staticmethod
    def get_all_disks(db_dir):
        disks = {}
        tables_dir = os.path.join(db_dir, "Tables")
        if os.path.exists(tables_dir) and os.path.isdir(tables_dir):
            for d in os.listdir(tables_dir):
                disks[d] = Disk(db_dir, d, -1, -1)

        return disks



    # returns base page set with associated tail page sets
    def read(self, base_rid):
        pass

    def write(self, base_rid):
        pass

    def __files_exist(self):
        return os.path.exists(self.base_fn) and os.path.exists(
            self.tail_fn) and os.path.exists(self.info_fn) and os.path.exists(self.key_directory)

    def __create_files(self, num_columns, key_column):
        if not os.path.exists(self.table_dir):
            os.makedirs(os.path.join(self.table_dir))  # make Tables directory for db if it doesn't exist
        b = open(self.tail_fn, "wb+")
        b.close()
        c = open(self.info_fn, "wb+")
        c.write(int.to_bytes(num_columns, length=8, byteorder="little"))
        c.write(int.to_bytes(key_column, length=8, byteorder="little"))
        c.write(int.to_bytes(START_RID, length=8, byteorder="little"))
        c.write(int.to_bytes(START_RID, length=8, byteorder="little"))
        c.close()
        self.__init_base_fn()  # initialize a page range worth of data for new table
        self.__init_key_directory()  # initialize a page range worth of page set data

    def __read_file_info(self):
        f = open(self.info_fn, "rb")
        self.num_columns = int.from_bytes(f.read(8), byteorder="little")
        self.key_column = int.from_bytes(f.read(8), byteorder="little")
        self.next_base_rid = int.from_bytes(f.read(8), byteorder="little")
        self.next_tail_rid = int.from_bytes(f.read(8), byteorder="little")
        f.close()

    def read_table(self):
        keys, base_block_start, tail_block_starts = self.__get_key_directory_data()
        table = Table(self.fn, self.num_columns, self.key_column)
        table.keys = keys
        table.brid_block_start = base_block_start
        table.trid_block_start = tail_block_starts
        return table

    def __get_key_directory_data(self):
        keys = {}
        base_block_starts = {}
        tail_block_starts = {}
        with open(self.key_directory, "rb") as f:
            while True:
                data = f.read(KEY_DIRECTORY_SET_SIZE)
                if data:
                    k = []
                    brids = []
                    trids = []
                    brid_block_starts = []
                    trid_block_starts = []
                    for i in range(RECORDS_PER_PAGE):
                        k.append(int.from_bytes(data[(i * 8):(i * 8) + 8], byteorder="little"))
                        brids.append(int.from_bytes(data[PAGE_SIZE + (i * 8):PAGE_SIZE + (i * 8) + 8], byteorder="little"))
                        trids.append(int.from_bytes(data[(PAGE_SIZE * 2) + (i * 8):(PAGE_SIZE * 2) + (i * 8) + 8], byteorder="little"))
                        brid_block_starts.append(int.from_bytes(data[(PAGE_SIZE * 3) + (i * 8):(PAGE_SIZE * 3) + (i * 8) + 8], byteorder="little"))
                        trid_block_starts.append(int.from_bytes(data[(PAGE_SIZE * 4) + (i * 8):(PAGE_SIZE * 4) + (i * 8) + 8], byteorder="little"))

                    for i in range(len(data) // 8):
                        keys[k[i]] = brids[i]
                        base_block_starts[brids[i]] = brid_block_starts[i]
                        tail_block_starts[trids[i]] = trid_block_starts[i]
                else:
                    break

        return keys, base_block_starts, tail_block_starts



    def read_base_page_set(self, block_start_index):
        pass

    def write_base_page_set(self, block_start_index):
        pass

    def read_tail_page_set(self, block_start_index):
        pass

    def write_tail_page_set(self, block_start_index):
        pass

    def __init_base_fn(self):
        with open(self.base_fn, "wb+") as f:
            for i in range(PAGE_SETS):
                f.write(bytearray((self.num_columns + META_DATA_PAGES) * PAGE_SIZE))

    def __init_key_directory(self):
        with open(self.key_directory, "wb+") as f:
            for i in range(PAGE_SETS):
                f.write(bytearray(KEY_DIRECTORY_SET_SIZE))

