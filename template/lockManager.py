from template.config import *
import threading


class LockManager:
    def __init__(self):
        self.x_locks = {}
        self.s_locks = {}

    def build_rid_lock(self, rid, set_type):
        if self.x_locks.get((rid, set_type)) is None:
            # build entry
            read_counter = 0
            self.x_locks[(rid, set_type)] = read_counter
        if self.s_locks.get((rid, set_type)) is None:
            # build entry
            read_counter = 0
            self.s_locks[(rid, set_type)] = read_counter

    def is_read_safe(self, rid, set_type):
        # if no write is occurring
        return self.x_locks.get(rid, set_type) == 0

    # mutex is acquired here
    def is_write_safe(self, rid, set_type):  # other info
        if self.s_locks.get(rid, set_type) != 0:
            return False  # a read it occurring
        return self.x_locks.get(rid, set_type) == 0

    def __increment_read_counter(self, rid, set_type):
        self.s_locks[(rid, set_type)] += 1
        pass

    def __decrement_read_counter(self, rid, set_type):
        self.s_locks[(rid, set_type)] -= 1
        if self.s_locks[(rid, set_type)] < 0:
            print("page_set_locks below 0 error: " + self.s_locks[(rid, set_type)])
            exit(-1)

    def __increment_write_counter(self, rid, set_type):
        self.x_locks[(rid, set_type)] += 1
        pass

    def __decrement_write_counter(self, rid, set_type):
        self.x_locks[(rid, set_type)] -= 1
        if self.x_locks[(rid, set_type)] < 0:
            print("page_set_locks below 0 error: " + self.s_locks[(rid, set_type)])
            exit(-1)
