import threading


class LockManager:
    def __init__(self):
        self.x_locks = {}
        self.s_locks = {}
        self.latches = {}
        self.build_latches()
        self.workers_list = []

    def build_latches(self):
        lower_bound = 40
        upper_bound = 49
        for i in range(lower_bound, upper_bound):
            self.latches[i] = threading.Lock()


    def build_rid_lock(self, rid, set_type):
        if self.x_locks.get((rid, set_type)) is None:
            self.x_locks[(rid, set_type)] = []
        if self.s_locks.get((rid, set_type)) is None:
            self.s_locks[(rid, set_type)] = []

    def is_read_safe(self, rid, set_type):
        val = self.x_locks.get((rid, set_type))
        if val:
            return val[0] == threading.currentThread().name

        return True

    # mutex is acquired here
    def is_write_safe(self, rid, set_type):
        shared = self.s_locks.get((rid, set_type))

        if len(shared) > 1:
            return False
        if shared:
            if shared[0] != threading.currentThread().name:
                return False

        exclusive = self.x_locks.get((rid, set_type))
        if exclusive:
            return exclusive[0] == threading.currentThread().name

        return True

    def __increment_read_counter(self, rid, set_type):
        self.s_locks[(rid, set_type)].append(threading.currentThread().name)

    def __decrement_read_counter(self, rid, set_type):
        self.s_locks[(rid, set_type)].remove(threading.currentThread().name)

    def __increment_write_counter(self, rid, set_type):
        self.x_locks[(rid, set_type)].append(threading.currentThread().name)

    def __decrement_write_counter(self, rid, set_type):
        self.x_locks[(rid, set_type)].remove(threading.currentThread().name)

    def acquire_write_lock(self, rid, set_type):
        self.build_rid_lock(rid, set_type)
        if not self.is_write_safe(rid, set_type):
            return False

        self.__increment_write_counter(rid, set_type)
        return True

    def acquire_read_lock(self, rid, set_type):
        self.build_rid_lock(rid, set_type)
        if not self.is_read_safe(rid, set_type):
            return False

        self.__increment_read_counter(rid, set_type)
        return True

    def abort(self, thread_name):
        self.__shrink(thread_name)

    def commit(self, thread_name):
        self.__shrink(thread_name)

    def __shrink(self, thread_name):
        x_keys = [k for k, v in self.x_locks.items() if thread_name in v]
        s_keys = [k for k, v in self.s_locks.items() if thread_name in v]

        for key in x_keys:
            self.x_locks[key].remove(thread_name)
        for key in s_keys:
            self.s_locks[key].remove(thread_name)
        pass

    def add_to_thread_list(self, thread):
        self.workers_list.append(thread)