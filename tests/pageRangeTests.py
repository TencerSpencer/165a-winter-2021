import unittest
from template.pageRange import PageRange


class PageRangeTests(unittest.TestCase):
    def test_create_page_range(self):
        pr = PageRange(5)
        self.assertTrue(pr.num_base_records == 0)

    def test_add_1_record(self):
        pr = PageRange(5)
        pr.add_record(0, [20, 21, 22, 23, 24])
        self.assertTrue(pr.num_base_records == 1)

    def test_add_10_records(self):
        pr = PageRange(5)
        columns = []
        for i in range(10):
            columns.append([20 + i, 21 + i, 22 + i, 23 + i, 24 + i])
            pr.add_record(i, columns[i])

        self.assertTrue(pr.num_base_records == 10)

    def test_add_8192_records(self):
        pr = PageRange(5)
        columns = []
        for i in range(8192):
            columns.append([20 + i, 21 + i, 22 + i, 23 + i, 24 + i])
            pr.add_record(i, columns[i])

        self.assertTrue(pr.num_base_records == 8192)

    def test_read_1_record(self):
        pr = PageRange(5)
        columns = [20, 21, 22, 23, 24]
        pr.add_record(0, columns)
        self.assertTrue(pr.num_base_records == 1)
        data = pr.get_record(0, [1, 1, 1, 1, 1])
        self.assertTrue(columns == data)

    def test_read_10_records(self):
        pr = PageRange(5)
        columns = []
        for i in range(10):
            columns.append([20 + i, 21 + i, 22 + i, 23 + i, 24 + i])
            pr.add_record(i, columns[i])

        read_data = []
        for i in range(10):
            read_data.append(pr.get_record(i, [1, 1, 1, 1, 1]))

        self.assertTrue(columns == read_data)

    def test_read_8192_records(self):
        pr = PageRange(5)
        columns = []
        for i in range(8192):
            columns.append([20 + i, 21 + i, 22 + i, 23 + i, 24 + i])
            pr.add_record(i, columns[i])

        read_data = []
        for i in range(8192):
            read_data.append(pr.get_record(i, [1, 1, 1, 1, 1]))

        self.assertTrue(columns == read_data)

    def test_update_1_record(self):
        pass

    def test_update_10_records(self):
        pass

    def test_update_8192_records(self):
        pass


if __name__ == '__main__':
    unittest.main()
