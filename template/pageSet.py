from template.page import Page


class PageSet:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.num_records = 0
        self.pages = []
        self.create_pages()
        self.rid = []
        self.schema_encoding = []
        self.indirection = []
        self.timestamp = []

    def create_pages(self):
        for i in range(self.num_columns):
            self.pages.append(Page())

    def add_record(self, *columns):
        for i in range(self.num_columns):
            self.pages[i].write(columns[i])
