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
        self.base_rids = {}  # key-value pairs: { rid : (0/1, page set index, offset) }
        self.tail_rids = {}
        self.base_schema_encodings = []
        self.base_indirections = []
        self.base_timestamps = []
        self.tail_schema_encodings = []
        self.tail_indirections = []
        self.tail_timestamps = []
        self.__init_base_page_sets()

    def __init_base_page_sets(self):
        for i in range(PAGE_SETS):
            self.base_page_sets.append(PageSet(self.num_columns))

    def __add_tail_page_set(self):
        self.tail_page_sets.append(PageSet(self.num_columns))

    def add_record(self, *columns):
        # add record to appropriate page set
        rid = self.create_rid()
        self.write_base_record(rid, columns)
        self.num_base_records += 1

    # setup is simplistic due to cumulative pages
    def get_record(self, rid, query_columns):
        page_set_index = self.base_rids[rid][1]

        # start at the base page, get its schema too
        base_page_set = self.base_page_sets[page_set_index]
        tail_page = base_page_set.get_indirection(rid)

        # get only the base page's info
        if tail_page is None:
            return self.get_only_base_record(rid, page_set_index, query_columns)
        else:
            tail_page_set_index = self.num_tail_records / RECORDS_PER_PAGE
            tail_page_set = self.tail_page_sets[tail_page_set_index]

            read_data = []

            # obtain schema
            base_page_offset = base_page_set.rids[rid][2]
            base_page_schema = base_page_set.schema_encoding[base_page_offset]

            # filler function that must obtain a tail page handler from a given RID
            tail_page_rid = self.base_indirections[self.base_rids[rid][2]]

            # pointer access isn't as expensive, utilize this to alternate page reads
            for i in range(self.num_columns):
                if (query_columns[i] != None):
                    if (base_page_schema >> i) == 1:
                        read_data.append(tail_page_set.read_record(tail_page_rid, query_columns, i))
                    else:
                        read_data.append(base_page_set.read_record(rid, query_columns, i))

            return read_data

    def get_only_base_record(self, rid, page_set_index, query_columns):

        read_data = []
        base_page_to_read = self.base_page_sets[page_set_index]
        for i in range(self.num_columns):
            read_data.append(base_page_to_read.read_record(rid, query_columns, i))

        return read_data

    def remove_record(self):
        # set given RID to null in directory
        pass

    def update_record(self, rid, page_set_index, *columns):

        # look for next available tail page, create one if it does not exist
        # get the next available RID and map it to a page in the given tail page
        # CUMULATIVE APPROACH: look at columns info and update it with the latest tail page's info
        # generate a new schema based on this UPDATED column and then have all this data get written to the tail page
        # set tail's schema and base's schema to the newly generated schema
        # if a previous tail existed, set that tail's indirection to the current tail
        # set the base's indirection to the new tail and the new tail's indirection to the base tail

        # I think that covers everything

        offset = self.base_rids[rid][2]
        updated_schema = 0
        for i in range(len(columns)):
            if columns[
                i] is not None:  # We are appending, not overwriting, yet we need to fix our pointers afterwards and move things upstream
                #   self.pages[i].update(columns[i], offset)

                # append a 1 in the location where the column has been updated
                updated_schema = (1 << i) | updated_schema
        self.base_schema_encodings[offset] = updated_schema

        pass

    def create_rid(self):  # When we merge, do we continue to keep our pages?
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def get_next_free_base_page_set(self):
        return self.num_base_records / RECORDS_PER_PAGE

    def has_space(self):
        if self.num_base_records < PAGE_SETS * RECORDS_PER_PAGE:
            return True

        return False

    def write_base_record(self, rid, *columns):
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
        self.base_indirections[offset] = None

        # store timestamp in milliseconds as an integer
        # https://www.tutorialspoint.com/How-to-get-current-time-in-milliseconds-in-Python#:~:text=You%20can%20get%20the%20current,1000%20and%20round%20it%20off.
        # To convert from milliseconds to date/time, https://stackoverflow.com/questions/748491/how-do-i-create-a-datetime-in-python-from-milliseconds
        self.base_timestamps[offset] = int(round(time.time() * 1000))

    def write_tail_record(self, rid, indirection, *columns):
        tail_page_set_index = self.num_tail_records / RECORDS_PER_PAGE
        tail_page_set = self.tail_page_sets[tail_page_set_index]

        # write data
        for i in range(self.num_columns):
            tail_page_set.pages[i].write(columns[i])

        # add key-value pair to base_rids where key is the rid and the value is the record offset
        self.tail_rids[rid] = (1, tail_page_set_index, self.num_tail_records)
        offset = self.tail_rids[rid][2]

        # add appropriate schema encoding, indirection, and timestamp
        updated_schema = 0
        for i in range(len(columns)):
            if columns[i] is not None:
                updated_schema = (1 << i) | updated_schema

        self.tail_schema_encodings[offset] = updated_schema
        self.tail_indirections[offset] = indirection
        self.tail_timestamps[offset] = int(round(time.time() * 1000))

    # Note, using cumulative upates
    # index will indicate where we read
    def read_record(self, rid, columns_to_read, index):
        offset = self.base_rids[rid]
        if columns_to_read is None:  # read all columns
            return self.pages[index].read(offset)
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
    def get_indirection(self, rid):
        offset = self.base_rids[rid][2]
        current_schema = self.base_schema_encodings[offset]
        # check current schema
        if (current_schema == 0):
            return None
        else:
            return self.base_indirections[offset]
