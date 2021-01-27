from template.page import Page
from template.config import *


class PageSet:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.pages = []
        self.__create_pages()

    def __create_pages(self):
        for i in range(self.num_columns):
            self.pages.append(Page())

    def add_record(self, *columns):
        for i in range(self.num_columns):
            self.pages[i].write(columns[i])

    def read_record(self, offset, query_columns):
        data = []
        for i in range(len(query_columns)):
            if query_columns[i] == 0:
                continue

            data.append(self.pages[i].read(offset))

        return data
