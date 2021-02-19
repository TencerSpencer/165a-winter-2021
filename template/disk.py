from template.config import *


class Disk:
    def __init__(self, table_name):
        self.fn = table_name
        self.base_fn = TABLE_BASE_PATH + table_name + ".base"
        self.tail_fn = TABLE_BASE_PATH + table_name + ".tail"
        self.info_fn = TABLE_BASE_PATH + table_name + ".info"
        self.base_meta_fn = TABLE_BASE_PATH + table_name + ".bmeta"
        self.tail_meta_fn = TABLE_BASE_PATH + table_name + ".tmeta"
        self.page_directory_fn = TABLE_BASE_PATH + table_name + ".pd"

    # returns base page set with associated tail page sets
    def read(self, base_rid):
        pass

    def write(self, base_rid):
        pass
