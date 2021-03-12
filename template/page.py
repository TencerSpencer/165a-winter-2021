from template.page_config import *
import threading

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.write_page_lock =  threading.Lock()

    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
        self.write_page_lock.acquire()
        value_in_bytes = []
        val = value
        for i in range(8):  # convert 64 bit value into byte sized chunks
            value_in_bytes.append((val >> 8*i) & 0b11111111)

        for i in range(8):  # add byte sized chunks to data
            self.data[(self.num_records * 8) + i] = value_in_bytes[i]

        self.num_records += 1
        self.write_page_lock.release()


    def write_to_offset(self, value, offset):
        self.write_page_lock.acquire()
        value_in_bytes = []
        val = value
        for i in range(8):  # convert 64 bit value into byte sized chunks
            value_in_bytes.append((val >> 8*i) & 0b11111111)

        for i in range(8):  # add byte sized chunks to data
            self.data[(offset * 8) + i] = value_in_bytes[i]

        self.num_records += 1
        self.write_page_lock.release()

    def overwrite(self, value, offset):
        self.write_page_lock.acquire()
        value_in_bytes = []
        val = value
        for i in range(8):  # convert 64 bit value into byte sized chunks
            value_in_bytes.append((val >> 8 * i) & 0b11111111)

        for i in range(8):  # add byte sized chunks to data
            self.data[(offset * 8) + i] = value_in_bytes[i]
        self.write_page_lock.release()

    def read(self, offset):
        self.write_page_lock.acquire()
        # convert bytes from data to a 64 bit integer and return value
        ret = int.from_bytes(self.data[(offset * 8):(offset * 8) + 8], "little")
        self.write_page_lock.release()
        return ret

