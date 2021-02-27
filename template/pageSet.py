from template.page import Page


class PageSet:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.pages = []
        self.__create_pages()

    def __create_pages(self):
        for i in range(self.num_columns):
            self.pages.append(Page())

    def has_capacity(self):
        return self.pages[0].has_capacity()

    def overwrite_base_record(self, data, offset):
        for i in range(self.num_columns):
            self.pages[i].overwrite(data[i], offset % 512)
