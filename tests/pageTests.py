import unittest
from template.page import Page


class PageTests(unittest.TestCase):
    def test_create_page(self):
        p = Page()
        self.assertTrue(p.num_records == 0)

    def test_write_one_integer(self):
        p = Page()
        p.write(69)
        self.assertTrue(p.num_records == 1)

    def test_read_one_integer(self):
        p = Page()
        p.write(69)
        self.assertEqual(69, p.read(0))

    def test_write_ten_integers(self):
        p = Page()
        for i in range(10):
            p.write(255 + i)

        self.assertTrue(p.num_records == 10)

    def test_read_ten_integers(self):
        p = Page()
        data = []
        actual = []
        for i in range(10):
            p.write(255 + i)
            actual.append(255 + i)

        for i in range(10):
            data.append(p.read(i))

        self.assertEqual(data, actual)

    def test_write_200_integers(self):
        p = Page()
        for i in range(200):
            p.write(234234 + i)

        self.assertTrue(p.num_records == 200)

    def test_read_200_integers(self):
        p = Page()
        data = []
        actual = []
        for i in range(200):
            p.write(234234 + i)
            actual.append(234234 + i)

        for i in range(200):
            data.append(p.read(i))

        self.assertEqual(p.num_records, 200)
        self.assertEqual(data, actual)

    def test_write_512_integers(self):
        p = Page()
        for i in range(512):
            p.write(234234 + i)

        self.assertTrue(p.num_records == 512)

    def test_read_512_integers(self):
        p = Page()
        data = []
        actual = []
        for i in range(512):
            p.write(234234 + i)
            actual.append(234234 + i)

        for i in range(512):
            data.append(p.read(i))

        self.assertEqual(p.num_records, 512)
        self.assertEqual(data, actual)


if __name__ == '__main__':
    unittest.main()
