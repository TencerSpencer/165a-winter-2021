from template.page import Page
from template.config import *
import time


class PageSet:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.num_records = 0
        self.pages = []
        self.rids = {}  # key-value pairs: { rid : offset }
        self.schema_encodings = []
        self.indirections = []
        self.timestamps = []
        self.__create_pages()

    def __create_pages(self):
        for i in range(self.num_columns):
            self.pages.append(Page())

    def add_record(self, rid, *columns):
        # write data
        for i in range(self.num_columns):
            self.pages[i].write(columns[i])

        # add key-value pair to rids where key is the rid and the value is a tuple containing the
        # base_page_set index and record offset
        self.rids[rid] = (self.num_columns / RECORDS_PER_PAGE, self.num_records)

        # add appropriate schema encoding, indirection, and timestamp
        self.schema_encodings[self.num_records] = '0' * self.num_columns
        self.indirections[self.num_records] = None
        self.timestamps[self.num_records] = time.strftime("%H:%M", time.localtime())

    def read_record(self, rid, query_columns):
        offset = self.rids[rid]
        data = []
        for i in range(len(query_columns)):
            if query_columns[i] == 0:
                continue

            data.append(self.pages[i].read(offset))

        return data
