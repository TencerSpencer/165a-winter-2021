from template.page import Page


class PageSet:

    def __init__(self, num_columns):
        self.num_pages = num_columns
        self.pages = []
        self.create_pages()
        self.rid = []
        self.schema_encoding = []
        self.indirection = []
        self.timestamp = []

    def create_pages(self):
        for i in range(self.num_pages):
            self.pages.append(Page())
