import copy, threading, time
from template.index import Index
from template.pageRange import PageRange
from template.config import *
from collections import deque


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
        self.key = key  # This is the index of the table key in columns that are sent in
        self.num_columns = num_columns
        self.keys = {}  # key-value pairs { key : rid }
        self.brid_block_start = {}  # { base rid : block start index }
        self.trid_block_start = {}  # { tail rid : block start index }
        self.brid_to_trid = {}  # { base rid : latest tail rid }
        self.page_directory = {}  # key-value pairs { rid : (page range index, base page set index) }
        self.index = None
        self.next_base_rid = START_RID
        self.next_tail_rid = START_RID
        self.page_ranges = {}
        self.disk = None
        self.merge_handler = MergeHandler()
        # when we start the timer, any class variables assigned after may not be captured, so do it at the end
        self.merge_handler.thread = threading.Timer(self.merge_handler.next_time_to_call - time.time(),
                                                    self.__merge_callback)
        #self.merge_handler.thread.start()

    def set_index(self, index):
        self.index = index

    def select_record_using_rid(self, rid, query_columns):
        page_range_index = self.page_directory[rid][0]
        cur_page_range = self.page_ranges[page_range_index]
        data = cur_page_range.get_record(rid, query_columns)
        return rid, data

    def __load_record_from_disk(self, rid, page_range_index, set_type):
        block_start_index = self.brid_block_start[rid] if set_type == BASE_RID_TYPE else self.trid_block_start[rid]

        if set_type == BASE_RID_TYPE:
            page_set_index = block_start_index // (self.num_columns + META_DATA_PAGES)
            data = BUFFER_POOL.get_page_set(self.name, self.num_columns, self.disk, page_range_index, page_set_index,
                                            set_type, block_start_index)
            page_set, brids, times, schema, indir, indir_t = Bufferpool.unpack_data(data)
            if not self.page_ranges.get(page_range_index):  # check if page range exists
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
            self.__add_trids_to_key_directory_info(trids, block_start_index)

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

            self.merge_handler.thread = threading.Timer(self.merge_handler.next_time_to_call - time.time(),
                                                        self.__merge_callback)
            self.merge_handler.next_time_to_call = self.merge_handler.next_time_to_call + MERGE_TIMER_INTERVAL
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

                # no matter what, reinsert the base page set into the queue
                self.merge_handler.full_base_page_sets.append((curr_range, curr_base))

                # merge the selected base_page_set,

    def __merge(self, page_range_index, base_page_set_index):
        page_range = self.page_ranges[page_range_index]
        #base_page_set = copy.deepcopy(page_range.base_page_sets[base_page_set_index])
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
        for i in range(len(brids)):
            self.page_directory[brids[i]] = (page_range_index, page_set_index)
            if indir_t[i] == TAIL_RID_TYPE or indir_t[i] == DELETED_WT_RID_TYPE:
                self.brid_to_trid[brids[i]] = indir[i]
            else:
                self.brid_to_trid[brids[i]] = None

    def __add_trids_to_key_directory_info(self, trids, block_start_index):
        for rid in trids:
            self.trid_block_start[rid] = block_start_index

    def insert_record(self, *columns):
        key_col = self.key
        new_rid = self.__get_next_base_rid()
        col_list = list(columns)
        key = col_list[key_col]

        next_free_page_range_index = self.__get_next_available_page_range()
        next_free_base_page_set_index = self.page_ranges[next_free_page_range_index].get_next_free_base_page_set()

        BUFFER_POOL.pin_page_set(self.name, next_free_page_range_index, next_free_base_page_set_index, BASE_RID_TYPE)

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

        result = curr_page_range.add_record(new_rid, col_list), new_rid

        # check if base_page_set is full, if so, add to dequeue
        if not curr_page_range.base_page_sets[next_free_base_page_set_index].has_capacity():
            self.merge_handler.full_base_page_sets.append((next_free_page_range_index, next_free_base_page_set_index))

        BUFFER_POOL.unpin_page_set(self.name, next_free_page_range_index, next_free_base_page_set_index, BASE_RID_TYPE)

        return result

    def update_record(self, key, *columns):
        self.merge_handler.update_mutex.acquire(blocking=True)

        base_rid = self.keys[key]
        self.__check_if_base_loaded(base_rid)

        page_range_index, base_page_set_index = self.page_directory[base_rid]

        # pin base page

        BUFFER_POOL.pin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)

        tail_rid = self.brid_to_trid[base_rid]
        if tail_rid is not None:
            self.__check_if_tail_loaded(tail_rid, page_range_index)
            tail_rid = self.brid_to_trid[base_rid]
        new_tail_rid = self.__get_next_tail_rid()

        if self.__tail_page_sets_full(page_range_index):
            tail_page_set_index = len(self.page_ranges[page_range_index].tail_page_sets)
            page_set, _, _, _, _, _ = Bufferpool.unpack_data(
                BUFFER_POOL.get_new_free_mem_space(self.name, page_range_index, tail_page_set_index, self.num_columns,
                                                   TAIL_RID_TYPE))
            self.page_ranges[page_range_index].tail_page_sets[tail_page_set_index] = page_set

        tail_page_set_index = self.page_ranges[page_range_index].get_next_free_tail_page_set()
        result = self.page_ranges[page_range_index].update_record(base_rid, new_tail_rid, columns)

        # mark tail page set as dirty
        BUFFER_POOL.mark_as_dirty(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)
        BUFFER_POOL.mark_as_dirty(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)
        # pin new tail
        BUFFER_POOL.pin_page_set(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)
        current_tail_page_set = None
        if tail_rid is not None:
            BUFFER_POOL.pin_page_set(self.name, page_range_index, current_tail_page_set, TAIL_RID_TYPE)

        # update key directory data for tail
        self.brid_to_trid[base_rid] = new_tail_rid
        self.trid_block_start[new_tail_rid] = (new_tail_rid // RECORDS_PER_PAGE) * (self.num_columns + META_DATA_PAGES)

        # append to base RID to a set of RIDs to merge, only do so after update is done, but why does it seem like this is running first?
        self.merge_handler.outdated_offsets[base_rid] = (page_range_index, base_page_set_index)

        # unpin everything
        BUFFER_POOL.unpin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)
        BUFFER_POOL.unpin_page_set(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)
        if tail_rid is not None:
            BUFFER_POOL.unpin_page_set(self.name, page_range_index, current_tail_page_set, TAIL_RID_TYPE)

        self.merge_handler.update_mutex.release()
        return result

    def __tail_page_sets_full(self, page_range_index):
        if len(self.page_ranges[page_range_index].tail_page_sets) == 0:
            return True

        return not list(self.page_ranges[page_range_index].tail_page_sets.values())[-1].has_capacity()

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

        if not self.page_ranges[page_range_index].is_valid(brid):  # check if brid has been invalidated
            return False

            # get base page sets,
        base_page_set_index = cur_page_range.base_rids.get(brid)[0]

        BUFFER_POOL.pin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)

        data = cur_page_range.get_record(brid, query_columns)

        BUFFER_POOL.unpin_page_set(self.name, page_range_index, base_page_set_index, BASE_RID_TYPE)
        if tail_rid:
            BUFFER_POOL.unpin_page_set(self.name, page_range_index, tail_page_set_index, TAIL_RID_TYPE)

        return brid, data

    def __check_if_base_loaded(self, rid):
        page_dir_info = self.page_directory.get(rid)
        if not page_dir_info:
            page_range_index = rid // (RECORDS_PER_PAGE * PAGE_SETS)
            self.__load_record_from_disk(rid, page_range_index, BASE_RID_TYPE)

    def __check_if_tail_loaded(self, rid, page_range_index):
        # check if tail page page set needs to be loaded
        if rid:
            if not self.page_ranges[page_range_index].tail_rids.get(rid):
                self.__load_record_from_disk(rid, page_range_index, TAIL_RID_TYPE)

    def remove_record(self, key):
        if key in self.keys:
            brid = self.keys[key]
            self.__check_if_base_loaded(brid)
            page_range_index, page_set = self.page_directory[brid]
            offset = self.page_ranges[page_range_index].base_rids[brid][1]
            _, tail_rid = self.page_ranges[page_range_index].base_indirections[offset]
            if tail_rid:
                self.page_ranges[page_range_index].base_indirections[offset] = (DELETED_WT_RID_TYPE, tail_rid)
            else:
                self.page_ranges[page_range_index].base_indirections[offset] = (DELETED_NT_RID_TYPE, tail_rid)
            BUFFER_POOL.mark_as_dirty(self.name, page_range_index, page_set, TAIL_RID_TYPE)
            return True

        return False

    def __get_next_base_rid(self):
        current_rid = self.next_base_rid
        self.next_base_rid += 1
        return current_rid

    def __get_next_tail_rid(self):
        current_rid = self.next_tail_rid
        self.next_tail_rid += 1
        return current_rid

    # returns an index of the next available page range
    def __get_next_available_page_range(self):
        if len(self.page_ranges) == 0:
            # no current page range, build one for starters and append to list
            new_page_range = PageRange(self.num_columns)
            for i in range(PAGE_SETS):
                page_set, _, _, _, _, _ = Bufferpool.unpack_data(
                    BUFFER_POOL.get_new_free_mem_space(self.name, 0, i, self.num_columns, BASE_RID_TYPE))
                new_page_range.base_page_sets[i] = page_set
            self.page_ranges[0] = new_page_range
            # our index will be our only page range
            return 0

        # note, I assume that base records will be removed at some point, so I choose to loop through each
        for i in range(len(self.page_ranges)):
            if not self.page_ranges[i].is_full():
                return i

        # if we reached here, then all our current page ranges are full. As such, build a new one
        new_page_range = PageRange(self.num_columns)
        for i in range(PAGE_SETS):
            page_set, _, _, _, _, _ = Bufferpool.unpack_data(
                BUFFER_POOL.get_new_free_mem_space(self.name, len(self.page_ranges), i, self.num_columns,
                                                   BASE_RID_TYPE))
            new_page_range.base_page_sets[i] = page_set
        self.page_ranges[len(self.page_ranges)] = new_page_range

        # length returns an index + 1, so subtract one to compensate
        return len(self.page_ranges) - 1

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
