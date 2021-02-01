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

    def test_update_once_1_record(self):
        pr = PageRange(5)
        pr.add_record(0, [20, 21, 22, 23, 24])
        pr.update_record(0, 0, [0, 1, 2, 3, 4])
        data = pr.get_record(0, [1, 1, 1, 1, 1])
        self.assertTrue(data == [0, 1, 2, 3, 4])

    def test_update_once_10_records(self):
        pr = PageRange(5)
        columns = []
        for i in range(10):
            columns.append([20 + i, 21 + i, 22 + i, 23 + i, 24 + i])
            pr.add_record(i, columns[i])

        updated_columns = []
        for i in range(10):
            updated_columns.append([i, i * 2, i * 3, i * 4, i * 5])
            pr.update_record(i, i, updated_columns[i])

        data = []
        for i in range(10):
            data.append(pr.get_record(i, [1, 1, 1, 1, 1]))

        self.assertTrue(data == updated_columns)

    def test_update_once_8192_records(self):
        pr = PageRange(5)
        columns = []
        for i in range(8192):
            columns.append([20 + i, 21 + i, 22 + i, 23 + i, 24 + i])
            pr.add_record(i, columns[i])

        updated_columns = []
        for i in range(8192):
            updated_columns.append([i, i * 2, i * 3, i * 4, i * 5])
            pr.update_record(i, i, updated_columns[i])

        data = []
        for i in range(8192):
            data.append(pr.get_record(i, [1, 1, 1, 1, 1]))

        self.assertTrue(data == updated_columns)

    def test_update_twice_1_record_1(self):
        pr = PageRange(5)
        update = [0, 1, 2, 3, 4]
        pr.add_record(0, [20, 21, 22, 23, 24])
        pr.update_record(0, 0, update)
        self.assertTrue(pr.num_tail_records == 1)
        update = [500, 600, 700, 800, 900]
        pr.update_record(0, 1, update)
        data = pr.get_record(0, [1, 1, 1, 1, 1])
        self.assertTrue(pr.num_base_records == 1)
        self.assertTrue(pr.num_tail_records == 2)
        self.assertTrue(data == update)

    def test_update_twice_1_record_2(self):
        pr = PageRange(5)
        update = [0, 1, 2, 3, 4]
        pr.add_record(0, [20, 21, 22, 23, 24])
        pr.update_record(0, 0, update)
        self.assertTrue(pr.num_tail_records == 1)
        update = [500, None, 700, None, 900]
        pr.update_record(0, 1, update)
        data = pr.get_record(0, [1, 1, 1, 1, 1])
        self.assertTrue(pr.num_base_records == 1)
        self.assertTrue(pr.num_tail_records == 2)
        self.assertTrue(data == [500, 1, 700, 3, 900])

    def test_update_one_column_1_record(self):
        pr = PageRange(5)
        update = [0, None, None, None, None]
        pr.add_record(0, [20, 21, 22, 23, 24])
        pr.update_record(0, 0, update)
        update = [0, 21, 22, 23, 24]
        data = pr.get_record(0, [1, 1, 1, 1, 1])
        self.assertTrue(data == update)

    def test_update_different_columns_1_record(self):
        pr = PageRange(5)
        update = [0, None, None, None, None]
        pr.add_record(0, [20, 21, 22, 23, 24])
        pr.update_record(0, 0, update)
        data = pr.get_record(0, [1, 1, 1, 1, 1])
        self.assertTrue(pr.num_base_records == 1)
        self.assertTrue(pr.num_tail_records == 1)
        self.assertTrue(data == [0, 21, 22, 23, 24])
        update = [0, 1, 2, 3, None]
        pr.update_record(0, 1, update)
        data = pr.get_record(0, [1, 1, 1, 1, 1])
        self.assertTrue(pr.num_base_records == 1)
        self.assertTrue(pr.num_tail_records == 2)
        self.assertTrue(data == [0, 1, 2, 3, 24])
        update = [None, None, None, None, 100]
        pr.update_record(0, 2, update)
        data = pr.get_record(0, [1, 1, 1, 1, 1])
        self.assertTrue(pr.num_base_records == 1)
        self.assertTrue(pr.num_tail_records == 3)
        self.assertTrue(data == [0, 1, 2, 3, 100])


if __name__ == '__main__':
    unittest.main()
