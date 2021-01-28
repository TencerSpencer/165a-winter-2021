import time
from template.page import Page


class PageSet:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.num_records = 0
        self.pages = []
        self.rids = {}  # key-value pairs: { rid : offset }
        self.schema_encodings = []
        self.indirections = []
        self.timestamps = []
        self.__create_pages()

    def __create_pages(self):
        for i in range(self.num_columns):
            self.pages.append(Page())

    def write_base_record(self, rid, *columns):
        # write data
        for i in range(self.num_columns):
            self.pages[i].write(columns[i])

        # add key-value pair to rids where key is the rid and the value is the record offset
        self.rids[rid] = self.num_records
        offset = self.rids[rid]

        # add appropriate schema encoding, indirection, and timestamp
        self.schema_encodings[offset] = 0
        self.indirections[offset] = None

        # store timestamp in milliseconds as an integer
        # https://www.tutorialspoint.com/How-to-get-current-time-in-milliseconds-in-Python#:~:text=You%20can%20get%20the%20current,1000%20and%20round%20it%20off.
        # To convert from milliseconds to date/time, https://stackoverflow.com/questions/748491/how-do-i-create-a-datetime-in-python-from-milliseconds
        self.timestamps[offset] = int(round(time.time() * 1000))

        self.num_records += 1

    def write_tail_record(self, rid, indirection, *columns):
        # write data
        for i in range(self.num_columns):
            self.pages[i].write(columns[i])

        # add key-value pair to rids where key is the rid and the value is the record offset
        self.rids[rid] = self.num_records
        offset = self.rids[rid]

        # add appropriate schema encoding, indirection, and timestamp
        updated_schema = 0
        for i in range(len(columns)):
            if columns[i] is not None:
                updated_schema = (1 << i) | updated_schema

        self.schema_encodings[offset] = updated_schema
        self.indirections[offset] = indirection
        self.timestamps[offset] = int(round(time.time() * 1000))

    # Note, using cumulative upates
    # index will indicate where we read
    def read_record(self, rid, columns_to_read, index):
        offset = self.rids[rid]
        if columns_to_read == None: # read all columns
            return self.pages[index].read(offset)
        else :
            return None
        

    def update_record(self, rid, *columns):
        
        offset = self.rids[rid]
        updated_schema = 0
        for i in range(len(columns)):
            if columns[i] is not None: # We are appending, not overwriting, yet we need to fix our pointers afterwards and move things upstream
                self.pages[i].update(columns[i], offset)

                # append a 1 in the location where the column has been updated
                updated_schema = (1 << i) | updated_schema
        self.schema_encodings[offset] = updated_schema

        # Need a new RID for tail page
        # Need indirection for base page i.e. we'll need to update the base page's RID each time
        # If a previous tail page existed, its indirection must be updated as well

    # this function may be depreciated soon
    # build an array of changed indices, useful for reads
    def __read_schema_encoding(self, rid):
        # using same lookup,
        offset = self.rids[rid]

        current_encoding = self.schema_encodings[offset]
        changed_index_arr = []

        # loop through schema and append to array if there is a 1
        for i in range(self.num_columns):
            if (current_encoding >> i) & 1:
                changed_index_arr.append(i)

        return changed_index_arr


    # return indirection RID of a page that has been updated
    def get_indirection(self, rid):
        offset = self.rids[rid]
        current_schema = self.schema_encodings[offset]
        # check current schema
        if (current_schema == 0):
            return None
        else :
            return self.indirections[offset]
