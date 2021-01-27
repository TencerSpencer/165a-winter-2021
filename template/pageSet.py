from template.page import Page
from template.config import *


class PageSet:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.pages = []
        self.create_pages()

    def create_pages(self):
        for i in range(self.num_columns):
            self.pages.append(Page())

    def add_record(self, *columns):
        for i in range(self.num_columns):
            self.pages[i].write(columns[i])
