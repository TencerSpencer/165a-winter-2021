from template.pageSet import PageSet
from template.config import *
import time
from template.lock_manager_config import *

class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.num_base_records = 0
        self.num_tail_records = 0
        self.base_page_sets = {}  # { page set index : page set }
        self.tail_page_sets = {}  # { page set index : page set }
        self.next_rid = 0
        self.base_rids = {}  # key-value pairs: { rid : (page set index, offset) }
        self.tail_rids = {}  # key-value pairs: { rid : (page set index, offset) }
        self.base_schema_encodings = {}
        self.base_indirections = {}  # contains (0/1 based on if base/tail, rid)
        self.base_timestamps = {}
        self.tail_schema_encodings = {}
        self.tail_indirections = {}  # contains (0/1 based on if base/tail, rid)
        self.tail_timestamps = {}

    def add_base_page_set_from_disk(self, page_set, page_set_index, brids, times, schema, indir, indir_t):
        self.num_base_records += len(brids)

        self.base_page_sets[page_set_index] = page_set

        base_rids = {}
        base_timestamps = {}
        base_schema_encodings = {}
        base_indirections = {}

        for i in range(len(brids)):
            internal_offset = brids[i] % (RECORDS_PER_PAGE * PAGE_SETS)
            base_rids[brids[i]] = (page_set_index, internal_offset)
            base_timestamps[internal_offset] = times[i]
            base_schema_encodings[internal_offset] = schema[i]
            base_indirections[internal_offset] = (indir_t[i], indir[i])

        self.base_rids.update(base_rids)
        self.base_timestamps.update(base_timestamps)
        self.base_schema_encodings.update(base_schema_encodings)
        self.base_indirections.update(base_indirections)

    def add_tail_page_set_from_disk(self, page_set, page_set_index, trids, times, schema, indir, indir_t):
        self.num_tail_records += len(trids)

        self.tail_page_sets[page_set_index] = page_set

        tail_rids = {}
        tail_timestamps = {}
        tail_schema_encodings = {}
        tail_indirections = {}

        for i in range(len(trids)):
            internal_offset = i + (RECORDS_PER_PAGE * page_set_index)
            tail_rids[trids[i]] = (page_set_index, internal_offset)
            tail_timestamps[internal_offset] = times[i]
            tail_schema_encodings[internal_offset] = schema[i]
            tail_indirections[internal_offset] = (indir_t[i], indir[i])

        self.tail_rids.update(tail_rids)
        self.tail_timestamps.update(tail_timestamps)
        self.tail_schema_encodings.update(tail_schema_encodings)
        self.tail_indirections.update(tail_indirections)

    def get_record_offset(self, rid, set_type):
        if set_type == BASE_RID_TYPE:
            return self.base_rids[rid][1]
        else:
            return self.tail_rids[rid][1]


    def get_next_tail_offset(self, tail_page_set_index):
        tail_page_set = self.tail_page_sets[tail_page_set_index]
        internal_offset = tail_page_set.pages[0].num_records
        return internal_offset + (RECORDS_PER_PAGE * tail_page_set_index)

    def add_record(self, rid, columns, base_page_set_index, base_record_offset):
        # LOCK_MANAGER.latches[WRITE_BASE_RECORD].acquire()

        # add record to appropriate page set
        self.__write_base_record(rid, columns, base_page_set_index, base_record_offset)
        # LOCK_MANAGER.latches[WRITE_BASE_RECORD].release()
        return True

    def get_record(self, base_record_rid, query_columns):
        base_page_set_index = self.base_rids[base_record_rid][0]

        # start at the base page, get its schema too
        tail_record_rid = self.__get_indirection(base_record_rid)[1]

        # get only the base page's info
        if tail_record_rid is None:
            return self.__get_only_base_record(base_record_rid, base_page_set_index, query_columns)
        else:
            tail_page_set_index = self.tail_rids[tail_record_rid][0]

            read_data = []

            # obtain schema
            base_record_offset = self.base_rids[base_record_rid][1]
            base_page_schema = self.base_schema_encodings[base_record_offset]

            # filler function that must obtain a tail page handler from a given RID
            tail_record_rid = self.base_indirections[base_record_offset][1]

            # pointer access isn't as expensive, utilize this to alternate page reads
            for i in range(self.num_columns):
                if query_columns[i] is not None:
                    if (base_page_schema >> self.num_columns - 1 - i) & 1:
                        read_data.append(self.__read_record(1, tail_record_rid, tail_page_set_index, i))
                    else:
                        read_data.append(self.__read_record(0, base_record_rid, base_page_set_index, i))
                else:
                    read_data.append(None)

            return read_data

    def get_record_with_specific_tail(self, base_record_rid, tail_record_rid, query_columns):
        base_page_set_index = self.base_rids[base_record_rid][0]

        # get only the base page's info
        if tail_record_rid is None:
            return self.__get_only_base_record(base_record_rid, base_page_set_index, query_columns)
        else:
            tail_page_set_index = self.tail_rids[tail_record_rid][0]

            read_data = []

            # obtain schema
            base_record_offset = self.base_rids[base_record_rid][1]
            base_page_schema = self.base_schema_encodings[base_record_offset]

            # pointer access isn't as expensive, utilize this to alternate page reads
            for i in range(self.num_columns):
                if query_columns[i] is not None:
                    if (base_page_schema >> self.num_columns - 1 - i) & 1:
                        read_data.append(self.__read_record(1, tail_record_rid, tail_page_set_index, i))
                    else:
                        read_data.append(self.__read_record(0, base_record_rid, base_page_set_index, i))
                else:
                    read_data.append(None)

            return read_data

    def __get_only_base_record(self, rid, page_set_index, query_columns):
        read_data = []
        for i in range(self.num_columns):
            if query_columns[i] is None:
                read_data.append(None)
            else:
                read_data.append(self.__read_record(0, rid, page_set_index, i))

        return read_data

    def remove_record(self, rid):
        self.base_rids.pop(rid)

    def update_record(self, base_rid, tail_rid, columns, tail_page_set_index):
        LOCK_MANAGER.latches[WRITE_BASE_RECORD].acquire()
        LOCK_MANAGER.latches[WRITE_TAIL_RECORD].acquire()
        # get previous tail rid
        prev_base_indirection = self.__get_indirection(base_rid)
        prev_tail_rid = prev_base_indirection[1]

        # get new schema for current update and base record
        base_record_offset = self.base_rids[base_rid][1]
        update_schema = 0
        for i in range(len(columns)):
            # We are appending, not overwriting, yet we need to fix our pointers afterwards and move things upstream
            if columns[i] is not None:
                # append a 1 in the location where the column has been updated
                update_schema = (1 << self.num_columns - 1 - i) | update_schema
        new_schema = update_schema | self.base_schema_encodings[base_record_offset]

        # update schema for base record
        self.base_schema_encodings[base_record_offset] = new_schema

        self.base_indirections[base_record_offset] = (1, tail_rid)

        # modify the columns to be written by merging the previous tail record and new columns to write
        new_columns = columns
        if prev_base_indirection != (None, None):
            new_columns = self.__get_new_columns_for_new_tail(prev_tail_rid, columns)

        # write new tail with new schema and previous tails rid
        self.__write_tail_record(tail_rid, new_schema,
                                 (0, base_rid) if prev_tail_rid is None else (1, prev_tail_rid),
                                 new_columns, tail_page_set_index)
        LOCK_MANAGER.latches[WRITE_TAIL_RECORD].release()
        LOCK_MANAGER.latches[WRITE_BASE_RECORD].release()
        return True

    def __get_new_columns_for_new_tail(self, prev_tail_rid, columns):
        data = list(columns)
        tail_page_set_index, offset = self.tail_rids[prev_tail_rid]
        prev_tail_schema = self.tail_schema_encodings[offset]
        offset = int(offset % RECORDS_PER_PAGE)  # get offset of the individual page

        # if a column in columns parameter is None, check if prev tail has a value for the column
        for i in range(self.num_columns):
            if data[i] is None:
                if (prev_tail_schema >> self.num_columns - 1 - i) & 1:
                    data[i] = self.tail_page_sets[tail_page_set_index].pages[i].read(offset)

        return data

    def has_space(self):
        return self.num_base_records < PAGE_SETS * RECORDS_PER_PAGE

    def __write_base_record(self, rid, columns, base_page_set_index, base_record_offset):
        base_page_set = self.base_page_sets[base_page_set_index]

        # write data
        for i in range(self.num_columns):
            base_page_set.pages[i].write_to_offset(columns[i], base_record_offset % RECORDS_PER_PAGE)

        # add key-value pair to base_rids where key is the rid and the value is the record offset
        self.base_rids[rid] = (base_page_set_index, base_record_offset)
        offset = self.base_rids[rid][1]

        # add appropriate schema encoding, indirection, and timestamp
        self.base_schema_encodings[offset] = 0
        self.base_indirections[offset] = (None, None)

        # store timestamp in milliseconds as an integer
        # https://www.tutorialspoint.com/How-to-get-current-time-in-milliseconds-in-Python#:~:text=You%20can%20get%20the%20current,1000%20and%20round%20it%20off.
        # To convert from milliseconds to date/time, https://stackoverflow.com/questions/748491/how-do-i-create-a-datetime-in-python-from-milliseconds
        self.base_timestamps[offset] = int(round(time.time() * 1000))


    def __write_tail_record(self, rid, schema, indirection, columns, tail_page_set_index):
        tail_page_set = self.tail_page_sets[tail_page_set_index]

        # write data
        internal_offset = tail_page_set.pages[0].num_records
        for i in range(self.num_columns):
            if columns[i] is not None:
                tail_page_set.pages[i].write(columns[i])
            else:
                tail_page_set.pages[i].write(0)  # make sure each tail page is synced with one another

        # add key-value pair to base_rids where key is the rid and the value is the record offset
        self.tail_rids[rid] = (tail_page_set_index, internal_offset + (RECORDS_PER_PAGE * tail_page_set_index))
        tail_record_offset = self.tail_rids[rid][1]

        self.tail_schema_encodings[tail_record_offset] = schema
        self.tail_indirections[tail_record_offset] = indirection
        self.tail_timestamps[tail_record_offset] = int(round(time.time() * 1000))

        self.num_tail_records += 1

    def __read_record(self, page_type, rid, page_set_index, column_to_read):
        if page_type == 0:
            offset = int(self.base_rids[rid][1] % RECORDS_PER_PAGE)  # get offset of the individual page
            if column_to_read is not None:  # read all columns
                return self.base_page_sets[page_set_index].pages[column_to_read].read(offset)
            else:
                return None
        else:
            offset = int(self.tail_rids[rid][1] % RECORDS_PER_PAGE)  # get offset of the individual page
            if column_to_read is not None:  # read all columns
                return self.tail_page_sets[page_set_index].pages[column_to_read].read(offset)
            else:
                return None

    # this function may be depreciated soon
    # build an array of changed indices, useful for reads
    def __read_schema_encoding(self, rid):
        # using same lookup,
        offset = self.base_rids[rid][1]

        current_encoding = self.base_schema_encodings[offset]
        changed_index_arr = []

        # loop through schema and append to array if there is a 1
        for i in range(self.num_columns):
            if (current_encoding >> i) & 1:
                changed_index_arr.append(i)

        return changed_index_arr

    # return indirection RID of a page that has been updated
    def __get_indirection(self, rid):
        base_record_offset = self.base_rids[rid][1]
        return self.base_indirections[base_record_offset]

    # Keep this function public so that table can check if this page range is full or not
    def is_full(self):
        return not self.num_base_records < PAGE_SETS * RECORDS_PER_PAGE

    def is_valid(self, rid):
        offset = self.base_rids[rid][1]
        return self.base_indirections[offset][0] != DELETED_WT_RID_TYPE and \
               self.base_indirections[offset][0] != DELETED_NT_RID_TYPE and \
               self.base_indirections[offset][0] != NO_RID_TYPE

    def rollback_indirection(self, base_rid):

        # get latest tail_rid
        offset = self.base_rids[base_rid][1]
        indirection = self.base_indirections[offset][1]

        # get the offset of the latest tail_rid
        tail_offset = self.tail_rids[indirection][1]
        indirection_prev_tail = self.tail_indirections[tail_offset]

        # swap indirection of base record to previous tail
        self.base_indirections[offset] = indirection_prev_tail 


        
    def rollback_base_deletion(self, base_rid):

        # get latest tail_rid
        offset = self.base_rids[base_rid][1]
        indirection = self.base_indirections[offset]

        if indirection[1] == DELETED_WT_RID_TYPE:
            self.base_indirections[offset][1] = (indirection[1], TAIL_RID_TYPE)

        elif indirection[1] == DELETED_NT_RID_TYPE:
            self.base_indirections[offset][1] = (indirection[1], BASE_RID_TYPE)

        else: 
            print("error with reverting deletion")
