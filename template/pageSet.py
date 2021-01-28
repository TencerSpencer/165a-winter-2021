import time
from template.page import Page


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

    def write_record(self, rid, *columns):
        # write data
        for i in range(self.num_columns):
            self.pages[i].write(columns[i])

        # add key-value pair to rids where key is the rid and the value is the record offset
        self.rids[rid] = self.num_records
        offset = self.rids[rid]

        # add appropriate schema encoding, indirection, and timestamp
        self.schema_encodings[offset] = '0' * self.num_columns
        self.indirections[offset] = None
        self.timestamps[offset] = time.strftime("%H:%M", time.localtime())

    def read_record(self, rid, query_columns):
        offset = self.rids[rid]
        data = []
        for i in range(len(query_columns)):
            if query_columns[i] == 0:
                continue

            data.append(self.pages[i].read(offset))

        return data

    def update_record(self, rid, *columns):
        offset = self.rids[rid]
        for i in range(len(columns)):
            if columns[i] is not None:
                self.pages[i].update(columns[i], offset)
