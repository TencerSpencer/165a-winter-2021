import threading

X_LOCK_EDIT = 55
S_LOCK_EDIT = 56
MERGE_MUTEX = 65

class LockManager:
    def __init__(self):
        self.x_locks = {}
        self.s_locks = {}
        self.latches = {}
        self.build_latches()
        self.workers_list = []
        self.merge_mutexes = {} # save each thread that requires an update here

    def build_latches(self):
        lower_bound = 40
        upper_bound = 70
        for i in range(lower_bound, upper_bound):
            self.latches[i] = threading.Lock()


    def build_rid_lock(self, rid, set_type):

        if self.x_locks.get((rid, set_type)) is None:
            self.latches[X_LOCK_EDIT].acquire()
            self.x_locks[(rid, set_type)] = []
            self.latches[X_LOCK_EDIT].release()
        if self.s_locks.get((rid, set_type)) is None:
            self.latches[S_LOCK_EDIT].acquire()
            self.s_locks[(rid, set_type)] = []
            self.latches[S_LOCK_EDIT].release()

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
        if threading.currentThread().name in self.s_locks[(rid, set_type)]:
            return

        self.latches[S_LOCK_EDIT].acquire()
        self.s_locks[(rid, set_type)].append(threading.currentThread().name)
        self.latches[S_LOCK_EDIT].release()

    def __decrement_read_counter(self, rid, set_type):
        self.latches[S_LOCK_EDIT].acquire()
        self.s_locks[(rid, set_type)].remove(threading.currentThread().name)
        self.latches[S_LOCK_EDIT].release()

    def __increment_write_counter(self, rid, set_type):
        if threading.currentThread().name in self.x_locks[(rid, set_type)]:
            return

        self.latches[X_LOCK_EDIT].acquire()
        self.x_locks[(rid, set_type)].append(threading.currentThread().name)
        self.latches[X_LOCK_EDIT].release()

    def __decrement_write_counter(self, rid, set_type):
        self.latches[X_LOCK_EDIT].acquire()
        self.x_locks[(rid, set_type)].remove(threading.currentThread().name)
        self.latches[X_LOCK_EDIT].release()

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

    def shrink(self, thread_name):
        self.latches[X_LOCK_EDIT].acquire()
        self.latches[S_LOCK_EDIT].acquire()
        x_keys = [k for k, v in self.x_locks.items() if thread_name in v]
        s_keys = [k for k, v in self.s_locks.items() if thread_name in v]
        self.latches[X_LOCK_EDIT].release()
        self.latches[S_LOCK_EDIT].release()

        for key in x_keys:
            self.x_locks[key].remove(thread_name)
        for key in s_keys:
            self.s_locks[key].remove(thread_name)
        pass

    def add_to_thread_list(self, thread):
        self.workers_list.append(thread)


    # functions to handle merging mutexes, used to speed up updates
    # merge_mutexes
    def acquire_mutex_for_update_thread(self):
        current_thread_name = threading.currentThread().name
        if self.merge_mutexes.get(current_thread_name) is None:
            self.latches[MERGE_MUTEX].acquire()
            # build the dictonary for it, make sure to add latch

            self.merge_mutexes[current_thread_name] = threading.Lock()
            self.latches[MERGE_MUTEX].release()
        
        self.merge_mutexes[current_thread_name].acquire(blocking=True)

    def release_mutex_for_update_thread(self):
        current_thread_name = threading.currentThread().name
        self.merge_mutexes[current_thread_name].release()

    def acquire_mutex_for_merge_thread(self):
        self.latches[MERGE_MUTEX].acquire()
        keys = self.merge_mutexes.keys()
        acquired_locks = []
        for key in keys:
            acquired_lock = self.merge_mutexes[key].acquire(blocking=False)
            # if we can acquire all locks, which means no update is occurring, then we can merge
            if acquired_lock == False:
                if len(acquired_locks) != 0:
                    for lock in acquired_locks:
                        # If we did not acquire all locks, release those that we did manage to acquire
                        self.merge_mutexes[lock].release()
                self.latches[MERGE_MUTEX].release()
                return False
            else:
                acquired_locks.append(key)
        self.latches[MERGE_MUTEX].release()
        return True


    # give up all mutexes
    def release_mutex_for_merge_thread(self):
        self.latches[MERGE_MUTEX].acquire()
        keys = self.merge_mutexes.keys()
        for key in keys:
            try:
                self.merge_mutexes[key].release()
            except RuntimeError:
                continue
        self.latches[MERGE_MUTEX].release()
