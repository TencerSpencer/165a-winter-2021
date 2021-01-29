import time
from template.page import Page


class PageSet:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.num_records = 0
        self.pages = []
        self.__create_pages()

    def __create_pages(self):
        for i in range(self.num_columns):
            self.pages.append(Page())