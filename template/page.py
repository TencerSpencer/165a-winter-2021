from template.page_config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
        value_in_bytes = []
        val = value
        for i in range(8):  # convert 64 bit value into byte sized chunks
            value_in_bytes.append((val >> 8*i) & 0b11111111)

        for i in range(8):  # add byte sized chunks to data
            self.data[(self.num_records * 8) + i] = value_in_bytes[i]

        self.num_records += 1

    def read(self, offset):
        # convert bytes from data to a 64 bit integer and return value
        return int.from_bytes(self.data[(offset * 8):(offset * 8) + 8], "little")

