from template.pageSet import PageSet
from template.config import *
import time


class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.num_base_records = 0
        self.num_tail_records = 0
        self.base_page_sets = []
        self.tail_page_sets = []
        self.next_rid = 0
        self.base_rids = {}  # key-value pairs: { rid : (page set index, offset) }
        self.tail_rids = {}  # key-value pairs: { rid : (page set index, offset) }
        self.base_schema_encodings = []
        self.base_indirections = []  # contains (0/1 based on if base/tail, rid)
        self.base_timestamps = []
        self.tail_schema_encodings = []
        self.tail_indirections = []  # contains (0/1 based on if base/tail, rid)
        self.tail_timestamps = []
        self.__init_base_page_sets()

    def __init_base_page_sets(self):
        for i in range(PAGE_SETS):
            self.base_page_sets.append(PageSet(self.num_columns))

    def __add_tail_page_set(self):
        self.tail_page_sets.append(PageSet(self.num_columns))

    def add_record(self, rid, *columns):
        if self.__is_full():
            return

        # add record to appropriate page set
        self.__write_base_record(rid, columns)
        self.num_base_records += 1

    # setup is simplistic due to cumulative pages
    def get_record(self, base_record_rid, query_columns):
        base_page_set_index = self.base_rids[base_record_rid][0]

        # start at the base page, get its schema too
        base_page_set = self.base_page_sets[base_page_set_index]
        tail_record_rid = base_page_set.get_indirection(base_record_rid)[1]

        # get only the base page's info
        if tail_record_rid is None:
            return self.__get_only_base_record(base_record_rid, base_page_set_index, query_columns)
        else:
            tail_page_set_index = self.tail_rids[tail_record_rid][0]
            tail_page_set = self.tail_page_sets[tail_page_set_index]

            read_data = []

            # obtain schema
            base_record_offset = base_page_set.rids[base_record_rid][1]
            base_page_schema = base_page_set.schema_encoding[base_record_offset]

            # filler function that must obtain a tail page handler from a given RID
            tail_record_rid = self.base_indirections[base_record_offset][1]

            # pointer access isn't as expensive, utilize this to alternate page reads
            for i in range(self.num_columns):
                if query_columns[i] is not None:
                    if (base_page_schema >> i) == 1:
                        read_data.append(tail_page_set[i].__read_record(1, tail_record_rid, tail_page_set_index, i))
                    else:
                        read_data.append(base_page_set[i].__read_record(0, base_record_rid, base_page_set_index, i))

            return read_data

    def __get_only_base_record(self, rid, page_set_index, query_columns):
        read_data = []
        base_page_to_read = self.base_page_sets[page_set_index]
        for i in range(self.num_columns):
            read_data.append(base_page_to_read.read_record(rid, query_columns, i))

        return read_data

    def remove_record(self, rid):
        self.base_rids.pop(rid)

    def update_record(self, base_rid, tail_rid, *columns):
        # look for next available tail page set, create one if it does not exist
        # get the next available RID and map it to a page in the given tail page
        # CUMULATIVE APPROACH: look at columns info and update it with the latest tail page's info
        # generate a new schema based on this UPDATED column and then have all this data get written to the tail page
        # set tail's schema and base's schema to the newly generated schema
        # if a previous tail existed, set that tail's indirection to the current tail
        # set the base's indirection to the new tail and the new tail's indirection to the previous tail
        # I think that covers everything

        # get previous tail rid
        prev_tail_rid = self.__get_indirection(base_rid)[1]

        # get new schema for current update and base record
        base_record_offset = self.base_rids[base_rid][1]
        update_schema = 0
        for i in range(len(columns)):
            # We are appending, not overwriting, yet we need to fix our pointers afterwards and move things upstream
            if columns[i] is not None:
                # append a 1 in the location where the column has been updated
                update_schema = (1 << i) | update_schema
        new_schema = update_schema | self.base_schema_encodings[base_record_offset]

        # update schema for base record
        self.base_schema_encodings[base_record_offset] = new_schema

        # change base and tail record indirections accordingly
        if prev_tail_rid is None:
            prev_tail_rid = base_rid
        self.base_indirections[base_record_offset] = (1, tail_rid)

        # modify the columns to be written by merging the previous tail record and new columns to write
        new_columns = columns
        if prev_tail_rid is not base_rid:
            new_columns = self.__get_new_columns_for_new_tail(prev_tail_rid, columns)

        # write new tail with new schema and previous tails rid
        self.__write_tail_record(tail_rid, new_schema, (0, prev_tail_rid) if prev_tail_rid is None else (1, prev_tail_rid), new_columns)

    def __get_new_columns_for_new_tail(self, prev_tail_rid, *columns):
        data = list(columns)
        tail_page_set_index, offset = self.tail_page_sets[prev_tail_rid][0:2]
        prev_tail_schema = self.tail_schema_encodings[offset]

        # if a column in columns parameter is None, check if prev tail has a value for the column
        for i in range(self.num_columns):
            if data[i] is None:
                if (prev_tail_schema >> i) & 1:
                    data[i] = self.tail_page_sets[tail_page_set_index][i].read(offset)

        return data

    def __get_next_free_base_page_set(self):
        return self.num_base_records / RECORDS_PER_PAGE

    def has_space(self):
        return self.num_base_records < PAGE_SETS * RECORDS_PER_PAGE

    def __write_base_record(self, rid, *columns):
        base_page_set_index = self.num_base_records / RECORDS_PER_PAGE
        base_page_set = self.base_page_sets[base_page_set_index]

        # write data
        for i in range(self.num_columns):
            base_page_set.pages[i].write(columns[i])

        # add key-value pair to base_rids where key is the rid and the value is the record offset
        self.base_rids[rid] = (0, base_page_set_index, self.num_base_records)
        offset = self.base_rids[rid]

        # add appropriate schema encoding, indirection, and timestamp
        self.base_schema_encodings[offset] = 0
        self.base_indirections[offset] = (None, None)

        # store timestamp in milliseconds as an integer
        # https://www.tutorialspoint.com/How-to-get-current-time-in-milliseconds-in-Python#:~:text=You%20can%20get%20the%20current,1000%20and%20round%20it%20off.
        # To convert from milliseconds to date/time, https://stackoverflow.com/questions/748491/how-do-i-create-a-datetime-in-python-from-milliseconds
        self.base_timestamps[offset] = int(round(time.time() * 1000))

    def __write_tail_record(self, rid, schema, indirection, *columns):
        if self.__tail_page_sets_full():
            self.tail_page_sets.append(PageSet(self.num_columns))

        tail_page_set_index = self.num_tail_records / RECORDS_PER_PAGE
        tail_page_set = self.tail_page_sets[tail_page_set_index]

        # write data
        for i in range(self.num_columns):
            tail_page_set.pages[i].write(columns[i])

        # add key-value pair to base_rids where key is the rid and the value is the record offset
        self.tail_rids[rid] = (1, tail_page_set_index, self.num_tail_records)
        tail_record_offset = self.tail_rids[rid][1]

        self.tail_schema_encodings[tail_record_offset] = schema
        self.tail_indirections[tail_record_offset] = indirection
        self.tail_timestamps[tail_record_offset] = int(round(time.time() * 1000))

        self.num_tail_records += 1

    def __read_record(self, page_type, rid, page_set_index, column_to_read):
        if page_type is 0:
            offset = self.base_rids[rid]
            if column_to_read is not None:  # read all columns
                return self.base_page_sets[page_set_index][column_to_read].read(offset)
            else:
                return None
        else:
            offset = self.tail_rids[rid]
            if column_to_read is not None:  # read all columns
                return self.tail_page_sets[page_set_index][column_to_read].read(offset)
            else:
                return None

    # this function may be depreciated soon
    # build an array of changed indices, useful for reads
    def __read_schema_encoding(self, rid):
        # using same lookup,
        offset = self.base_rids[rid]

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

    def __tail_page_sets_full(self):
        if len(self.tail_page_sets) is 0:
            return True

        return not self.tail_page_sets[-1].has_capacity()

    def __is_full(self):
        return self.num_base_records < PAGE_SETS * RECORDS_PER_PAGE
