from template.lock_manager_config import *
import copy, time
from template.pageRange import PageRange
from template.config import *
from collections import deque
import threading


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class MergeHandler:
    def __init__(self):
        # setup merge variables
        self.next_time_to_call = time.time()  # prepare consistent time callback
        self.thread_in_crit_section = False
        self.thread_stopped = False

        # prevent timer from executing if we are performing an update with a mutex
        # https://docs.python.org/3/library/threading.html#threading.Lock.acquire
        self.update_mutex = threading.Lock()

        # full base page sets, need to append upon insertion
        self.full_base_page_sets = deque()  # (page_range_offset, base_page_set_offset)

        # dict of base RID to offset in old copy of base record
        self.outdated_offsets = {}  # {base RID : page_range_offset, base_page_set_offset}

        # create background proc thread
        self.thread = None


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.keyLock = threading.Lock()
        self.key = key  # This is the index of the table key in columns that are sent in
        self.num_columns = num_columns
        self.keys = {}  # key-value pairs { key : rid }
        self.brid_block_start = {}  # { base rid : block start index }
        self.trid_block_start = {}  # { tail rid : block start index }
        self.brid_to_trid = {}  # { base rid : latest tail rid }
        self.page_directory = {}  # key-value pairs { rid : (page range index, base page set index) }
        self.tblock_directory = {}  # { (page range index, tail page set index) : block start index
        self.index = None
        self.next_base_rid = START_RID
        self.next_tail_rid = START_RID
        self.page_ranges = {}
        self.disk = None
        self.merge_handler = MergeHandler()
        # when we start the timer, any class variables assigned after may not be captured, so do it at the end
        self.merge_handler.thread = threading.Timer(self.merge_handler.next_time_to_call - time.time(),
                                                    self.__merge_callback)
        self.merge_handler.thread.start()

    def set_index(self, index):
        self.index = index

    def safe_get_keys(self):
        with self.keyLock:
            return copy.deepcopy(list(self.keys.keys()))

    def select_record_using_rid(self, rid, query_columns):
        self.__check_if_base_loaded(rid)
        page_range_index = self.page_directory[rid][0]
        cur_page_range = self.page_ranges[page_range_index]

        tail_rid = self.brid_to_trid[rid]
        if tail_rid is not None:
            self.__check_if_tail_loaded(tail_rid, page_range_index)
            tail_page_set_index = cur_page_range.tail_rids.get(tail_rid)[0]
            BUFFER_POOL.pin_page_set(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)

        if not self.page_ranges[page_range_index].is_valid(rid):  # check if brid has been invalidated
            return False

            # get base page sets,
        base_page_set_index = cur_page_range.base_rids.get(rid)[0]

        BUFFER_POOL.pin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)

        data = cur_page_range.get_record(rid, query_columns)

        BUFFER_POOL.unpin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)
        if tail_rid is not None:
            BUFFER_POOL.unpin_page_set(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)

        return rid, data

    def __load_record_from_disk(self, rid, page_range_index, set_type):
        block_start_index = self.brid_block_start[rid] if set_type == BASE_RID_TYPE else self.trid_block_start[rid]

        if set_type == BASE_RID_TYPE:
            page_set_index = block_start_index // (self.num_columns + META_DATA_PAGES)
            data = BUFFER_POOL.get_page_set(self.name, self.num_columns, self.disk, page_range_index, page_set_index,
                                            set_type, block_start_index)
            page_set, brids, times, schema, indir, indir_t = Bufferpool.unpack_data(data)
            if self.page_ranges.get(page_range_index) is None:  # check if page range exists
                self.page_ranges[page_range_index] = PageRange(self.num_columns)

            self.__add_brids_to_page_directory(brids, indir, indir_t, page_range_index, page_set_index)
            self.page_ranges[page_range_index].add_base_page_set_from_disk(page_set, page_set_index, brids,
                                                                           times, schema, indir, indir_t)
        else:
            page_set_index = len(self.page_ranges[page_range_index].tail_page_sets)
            data = BUFFER_POOL.get_page_set(self.name, self.num_columns, self.disk, page_range_index, page_set_index,
                                            set_type, block_start_index)
            page_set, trids, times, schema, indir, indir_t = Bufferpool.unpack_data(data)
            # this implementation will first be in mem
            self.page_ranges[page_range_index].add_tail_page_set_from_disk(page_set, page_set_index, trids,
                                                                           times, schema, indir, indir_t)

            # new_block_start_index = self.__get_tail_block(page_range_index, page_set_index)
            self.__add_trids_to_key_directory_info(trids, block_start_index)
        LOCK_MANAGER.build_rid_lock(rid, set_type)

    # for help with background process operation and to ensure timer is consistent
    # https://stackoverflow.com/questions/8600161/executing-periodic-actions-in-python
    def __merge_callback(self):
        if not self.merge_handler.thread_stopped:

            # if no base page set is full, do not perform a merge
            if len(self.merge_handler.full_base_page_sets) != 0:
                if (self.merge_handler.update_mutex.acquire(blocking=False) == True):
                    # call __check_for_merge
                    self.merge_handler.thread_in_crit_section = True
                    self.__check_for_merge()
                    self.merge_handler.thread_in_crit_section = False
                    self.merge_handler.update_mutex.release()

            self.merge_handler.next_time_to_call = self.merge_handler.next_time_to_call + MERGE_TIMER_INTERVAL
            self.merge_handler.thread = threading.Timer(self.merge_handler.next_time_to_call - time.time(),
                                                        self.__merge_callback)
            self.merge_handler.thread.start()

    def shut_down_timer(self):  # if constant printing is occurring, this wont properly stop
        if self.merge_handler.thread_in_crit_section:
            # kill a certain way
            self.merge_handler.thread_stopped = True
            self.merge_handler.thread.join()
        else:
            self.merge_handler.thread_stopped = True
            self.merge_handler.thread.cancel()
            # use this to halt timer, but does not halt thread as per documentation

    # IF HAVING ISSUES TO GET MERGES TO OCCUR, TRY ADJUSTING MERGE_TIMER_INTERVAL IN CONFIG.PY
    def __check_for_merge(self):
        if len(self.merge_handler.outdated_offsets) != 0:
            # dictonaries will error out if their size changes during copy, so use a mutex for copying
            #  self.merge_handler.dict_mutex.acquire()
            rid_dict = copy.deepcopy(self.merge_handler.outdated_offsets)
            self.merge_handler.outdated_offsets.clear()
            # self.merge_handler.dict_mutex.release()

            check_num = NUMBER_OF_BASE_PAGE_SETS_TO_CHECK if len(self.merge_handler.full_base_page_sets) >= 3 else len(
                self.merge_handler.full_base_page_sets)
            for _ in range(check_num):
                # Compare offsets between full_base_page_sets and outdated_offsets to see if we have a sufficient amount of updates
                curr_range, curr_base = self.merge_handler.full_base_page_sets.popleft()
                if list(rid_dict.values()).count((curr_range, curr_base)) >= MERGE_THRESHOLD:
                    # merge base_page_set
                    self.__merge(curr_range, curr_base)
                    # print("MERGE OCCURRED")

                # no matter what, reinsert the base page set into the queue
                self.merge_handler.full_base_page_sets.append((curr_range, curr_base))

                # merge the selected base_page_set,

    def __merge(self, page_range_index, base_page_set_index):
        page_range = self.page_ranges[page_range_index]
        # base_page_set = copy.deepcopy(page_range.base_page_sets[base_page_set_index])
        brids = [k for k, v in self.page_directory.items() if v[0] == page_range_index and v[1] == base_page_set_index]
        self.__check_if_base_loaded(brids[0])  # just need first one since all brids are in same page set
        for i in range(len(brids)):
            _, offset = page_range.base_rids[brids[i]]
            if page_range.base_schema_encodings[offset] != 0:
                tail_rid = page_range.base_indirections[offset][1]
                self.__check_if_tail_loaded(tail_rid, page_range_index)
                data = page_range.get_record_with_specific_tail(brids[i], tail_rid, [1] * self.num_columns)
                page_range.base_page_sets[base_page_set_index].overwrite_base_record(data, offset)

    # page_range.base_page_sets[base_page_set_index] = base_page_set

    def __add_brids_to_page_directory(self, brids, indir, indir_t, page_range_index, page_set_index):
        # If we are using self.table.keys.keys() for things, we need to lock whenever we make additions to the table, i.e. here
        #LOCK_MANAGER.latches[PAGE_DIR].acquire()
        for i in range(len(brids)):
            self.page_directory[brids[i]] = (page_range_index, page_set_index)
            if indir_t[i] == TAIL_RID_TYPE or indir_t[i] == DELETED_WT_RID_TYPE:
                self.brid_to_trid[brids[i]] = indir[i]
            else:
                self.brid_to_trid[brids[i]] = None

        #LOCK_MANAGER.latches[PAGE_DIR].release()

    def __add_trids_to_key_directory_info(self, trids, block_start_index):
        for rid in trids:
            self.trid_block_start[rid] = block_start_index

    def insert_record(self, *columns):
        # TODO: need to return false if key already exists before incrementing rid
        key_col = self.key
        cols = list(columns)
        key = cols[key_col]

        LOCK_MANAGER.latches[NEW_BASE_RID_INSERT].acquire()
        new_rid = self.next_base_rid

        next_free_page_range_index = self.__get_next_available_page_range(new_rid)
        next_free_base_page_set_index = (new_rid // RECORDS_PER_PAGE) % PAGE_SETS

        if not LOCK_MANAGER.acquire_write_lock(new_rid, BASE_RID_TYPE):
            # NOTE: IT IS VERY IMPORTANT TO RELEASE HERE IF WE CANNOT ACQUIRE A WRITE LOCK.
            LOCK_MANAGER.latches[NEW_BASE_RID_INSERT].release()
            return False

        self.__increment_base_rid()
        LOCK_MANAGER.latches[NEW_BASE_RID_INSERT].release()

        BUFFER_POOL.pin_page_set(self.name, next_free_page_range_index, next_free_base_page_set_index, BASE_RID_TYPE)

        # Moved this beyond the lock manager latch to lower runtime
        # check if we need to load previous rid's page set since it could be incomplete
        self.__check_if_base_loaded(new_rid - 1)

        # set key and rid mappings
        self.keys[key] = new_rid
        self.page_directory[new_rid] = (next_free_page_range_index, next_free_base_page_set_index)

        # continue with inserting the record here
        curr_page_range = self.page_ranges[next_free_page_range_index]

        # mark page set as dirty
        BUFFER_POOL.mark_as_dirty(self.name, next_free_page_range_index, next_free_base_page_set_index, BASE_RID_TYPE)

        # update key directory data for base
        self.brid_to_trid[new_rid] = None
        self.brid_block_start[new_rid] = (new_rid // RECORDS_PER_PAGE) * (self.num_columns + META_DATA_PAGES)

        result = curr_page_range.add_record(new_rid, cols, next_free_base_page_set_index), new_rid

        # check if base_page_set is full, if so, add to dequeue
        if not curr_page_range.base_page_sets[next_free_base_page_set_index].has_capacity():
            self.merge_handler.full_base_page_sets.append((next_free_page_range_index, next_free_base_page_set_index))

        BUFFER_POOL.unpin_page_set(self.name, next_free_page_range_index, next_free_base_page_set_index, BASE_RID_TYPE)

        return result

    def update_record(self, key, *columns):
        # get merge handler mutex
        self.merge_handler.update_mutex.acquire(blocking=True)

        base_rid = self.keys[key]
        self.__check_if_base_loaded(base_rid)

        page_range_index, base_page_set_index = self.page_directory[base_rid]

        # pin base page
        BUFFER_POOL.pin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)

        prev_tail_rid = self.brid_to_trid[base_rid]
        if prev_tail_rid is not None:
            self.__check_if_tail_loaded(prev_tail_rid, page_range_index)
            prev_tail_rid = self.brid_to_trid[base_rid]
            # no need to block previous tails
        
        LOCK_MANAGER.latches[NEW_TAIL_RID_UPDATE].acquire()
        new_tail_rid = self.next_tail_rid

        tail_page_set_index = new_tail_rid // RECORDS_PER_PAGE
        if self.page_ranges[page_range_index].tail_page_sets.get(tail_page_set_index) is None:
            page_set, _, _, _, _, _ = Bufferpool.unpack_data(
                BUFFER_POOL.get_new_free_mem_space(self.name, page_range_index, tail_page_set_index, self.num_columns,
                                                   TAIL_RID_TYPE))
            self.page_ranges[page_range_index].tail_page_sets[tail_page_set_index] = page_set
        elif self.__tail_page_sets_full(tail_page_set_index, page_range_index):
            tail_page_set_index += 1
            page_set, _, _, _, _, _ = Bufferpool.unpack_data(
                BUFFER_POOL.get_new_free_mem_space(self.name, page_range_index, tail_page_set_index, self.num_columns,
                                                   TAIL_RID_TYPE))
            self.page_ranges[page_range_index].tail_page_sets[tail_page_set_index] = page_set

        if not LOCK_MANAGER.acquire_write_lock(new_tail_rid, TAIL_RID_TYPE) or \
                not LOCK_MANAGER.acquire_read_lock(base_rid, BASE_RID_TYPE):
            LOCK_MANAGER.latches[NEW_TAIL_RID_UPDATE].release()
            return False

        self.__increment_tail_rid()
        LOCK_MANAGER.latches[NEW_TAIL_RID_UPDATE].release()
        #LOCK_MANAGER.__increment_write_counter(base_rid, BASE_RID_TYPE)
        #LOCK_MANAGER.__increment_write_counter(new_tail_rid, TAIL_RID_TYPE)

        result = self.page_ranges[page_range_index].update_record(base_rid, new_tail_rid, columns, tail_page_set_index)

        # mark tail page set as dirty
        BUFFER_POOL.mark_as_dirty(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)
        BUFFER_POOL.mark_as_dirty(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)
        # pin new tail
        BUFFER_POOL.pin_page_set(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)
        current_tail_page_set = None
        if prev_tail_rid is not None:
            # prev_tail_page_set = self.page_ranges[page_range_index].tail_rids[prev_tail_rid][0]
            BUFFER_POOL.pin_page_set(self.name, page_range_index, current_tail_page_set, TAIL_RID_TYPE)

        # update key directory data for tail
        self.brid_to_trid[base_rid] = new_tail_rid
        self.trid_block_start[new_tail_rid] = self.__get_tail_block(page_range_index, tail_page_set_index)

        # append to base RID to a set of RIDs to merge, only do so after update is done, but why does it seem like this is running first?
        self.merge_handler.outdated_offsets[base_rid] = (page_range_index, base_page_set_index)

        # unpin everything
        BUFFER_POOL.unpin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)
        BUFFER_POOL.unpin_page_set(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)
        if prev_tail_rid is not None:
            BUFFER_POOL.unpin_page_set(self.name, page_range_index, current_tail_page_set, TAIL_RID_TYPE)

        # self.is_write_safe(page_range_index, tail_page_set_index, TAIL_RID_TYPE) &
        # self.is_read_safe(page_range_index, base_page_set_index, BASE_RID_TYPE)):

        self.merge_handler.update_mutex.release()

        return result

    def __get_tail_block(self, page_range_index, tail_page_set):
        LOCK_MANAGER.latches[NEXT_TAIL_BLOCK_CALC].acquire()
        block = None
        if self.tblock_directory.get((page_range_index, tail_page_set)) is None:
            block = self.disk.get_next_tail_block()
            self.tblock_directory[(page_range_index, tail_page_set)] = block

        else:
            block = self.tblock_directory[(page_range_index, tail_page_set)]
        
        LOCK_MANAGER.latches[NEXT_TAIL_BLOCK_CALC].release()
        return block

    # Possible mutex here due to length check
    def __tail_page_sets_full(self, page_set_index, page_range_index):
        if len(self.page_ranges[page_range_index].tail_page_sets) == 0:
            return True

        return not self.page_ranges[page_range_index].tail_page_sets[page_set_index].has_capacity()

    def select_record(self, key, query_columns):
        if key not in self.keys:
            return False

        brid = self.keys[key]
        self.__check_if_base_loaded(brid)
        page_range_index = self.page_directory[brid][0]
        cur_page_range = self.page_ranges[page_range_index]

        tail_rid = self.brid_to_trid[brid]
        if tail_rid is not None:
            self.__check_if_tail_loaded(tail_rid, page_range_index)
            tail_page_set_index = cur_page_range.tail_rids.get(tail_rid)[0]
            BUFFER_POOL.pin_page_set(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)
            # check if tail page set can be read
            if not LOCK_MANAGER.acquire_read_lock(tail_rid, TAIL_RID_TYPE):
                return False

        if not self.page_ranges[page_range_index].is_valid(brid):  # check if brid has been invalidated
            return False

        # get base page sets,
        base_page_set_index = cur_page_range.base_rids.get(brid)[0]

        # check is base page set can be read
        if not LOCK_MANAGER.acquire_read_lock(brid, BASE_RID_TYPE):
            return False

        BUFFER_POOL.pin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)

        data = cur_page_range.get_record(brid, query_columns)

        BUFFER_POOL.unpin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)
        if tail_rid is not None:
            BUFFER_POOL.unpin_page_set(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)

        return brid, data

    def __check_if_base_loaded(self, rid):
        page_dir_info = self.page_directory.get(rid)
        LOCK_MANAGER.latches[BASE_LOADED].acquire()
        if not page_dir_info and self.brid_to_trid.get(rid) is not None:
            page_range_index = rid // (RECORDS_PER_PAGE * PAGE_SETS)
            self.__load_record_from_disk(rid, page_range_index, BASE_RID_TYPE)
        LOCK_MANAGER.latches[BASE_LOADED].release()

    def __check_if_tail_loaded(self, rid, page_range_index):
        LOCK_MANAGER.latches[TAIL_LOADED].acquire()
        # check if tail page page set needs to be loaded
        if rid:
            if self.page_ranges[page_range_index].tail_rids.get(rid) is None and self.trid_block_start.get(rid) is not None:
                self.__load_record_from_disk(rid, page_range_index, TAIL_RID_TYPE)
        LOCK_MANAGER.latches[TAIL_LOADED].release()

    def remove_record(self, key):
        if key in self.keys:
            brid = self.keys[key]
            self.__check_if_base_loaded(brid)
            page_range_index, page_set = self.page_directory[brid]
            offset = self.page_ranges[page_range_index].base_rids[brid][1]
            _, tail_rid = self.page_ranges[page_range_index].base_indirections[offset]

            # if we cannot write to the current base_indirection
            if not LOCK_MANAGER.acquire_write_lock(brid, BASE_RID_TYPE) or\
                not LOCK_MANAGER.acquire_write_lock(tail_rid, TAIL_RID_TYPE):
                return False

            if tail_rid:
                LOCK_MANAGER.__increment_write_counter(tail_rid, TAIL_RID_TYPE)
                self.page_ranges[page_range_index].base_indirections[offset] = (DELETED_WT_RID_TYPE, tail_rid)
            else:
                self.page_ranges[page_range_index].base_indirections[offset] = (DELETED_NT_RID_TYPE, tail_rid)
            # swapped from TAIL_RID_TYPE to BASE_RID_TYPE.
            BUFFER_POOL.mark_as_dirty(self.name, page_range_index, page_set, BASE_RID_TYPE)

            return True

        return False

    def __increment_base_rid(self):
        self.next_base_rid += 1

    def __increment_tail_rid(self):
        self.next_tail_rid += 1

    # returns an index of the next available page range
    def __get_next_available_page_range(self, new_rid):

        # For optimization purposes, we must lessen the how often we use latches
        page_range_index = new_rid // (RECORDS_PER_PAGE * PAGE_SETS)
        LOCK_MANAGER.latches[AVAILABLE_PAGE_RANGE].acquire()     
        if self.page_ranges.get(page_range_index) is None:  # page range doesn't exist
            new_page_range = PageRange(self.num_columns)
            for i in range(PAGE_SETS):
                page_set, _, _, _, _, _ = Bufferpool.unpack_data(
                    BUFFER_POOL.get_new_free_mem_space(self.name, page_range_index, i, self.num_columns, BASE_RID_TYPE))
                new_page_range.base_page_sets[i] = page_set
            page_range_index = new_rid // (RECORDS_PER_PAGE * PAGE_SETS)
            self.page_ranges[page_range_index] = new_page_range
        
        LOCK_MANAGER.latches[AVAILABLE_PAGE_RANGE].release()
        return page_range_index

    def get_meta_data(self, page_range_index, page_set_index, set_type):
        page_range = self.page_ranges[page_range_index]
        timestamps = []
        schema = []
        indir = []
        indir_t = []
        if set_type == BASE_RID_TYPE:
            rids = [k for k, v in page_range.base_rids.items() if v[0] == page_set_index]
            for i in range(len(rids)):
                offset = page_range.base_rids[rids[i]][1]
                timestamps.append(page_range.base_timestamps[offset])
                schema.append(page_range.base_schema_encodings[offset])
                temp = page_range.base_indirections[offset]
                indir.append(temp[1])
                indir_t.append(temp[0])
        else:
            rids = [k for k, v in page_range.tail_rids.items() if v[0] == page_set_index]
            for i in range(len(rids)):
                offset = page_range.tail_rids[rids[i]][1]
                timestamps.append(page_range.tail_timestamps[offset])
                schema.append(page_range.tail_schema_encodings[offset])
                temp = page_range.tail_indirections[offset]
                indir.append(temp[1])
                indir_t.append(temp[0])

        return rids, timestamps, schema, indir, indir_t
