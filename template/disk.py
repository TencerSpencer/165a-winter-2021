from template.config import *


class Disk:
    def __init__(self, file_name):
        self.fn = file_name
        self.base_fn = TABLE_BASE_PATH + file_name + ".base"
        self.tail_fn = TABLE_BASE_PATH + file_name + ".tail"
        self.info_fn = TABLE_BASE_PATH + file_name + ".info"
        self.base_meta_fn = TABLE_BASE_PATH + file_name + ".bmeta"
        self.tail_meta_fn = TABLE_BASE_PATH + file_name + ".tmeta"
        self.page_directory_fn = TABLE_BASE_PATH + file_name + ".pd"

    # returns base page set with associated tail page sets
    def read(self, base_rid):
        pass

    def write(self, base_rid):
        pass
