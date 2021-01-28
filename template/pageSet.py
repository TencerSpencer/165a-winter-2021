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

    def write_record(self, rid, *columns):
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


    def read_record(self, rid, query_columns):
        offset = self.rids[rid]
        data = []
        for i in range(len(query_columns)):
            if query_columns[i] == 0:
                data.append(None)

            data.append(self.pages[i].read(offset))

        return data

    def update_record(self, rid, *columns):
        offset = self.rids[rid]
        updated_schema = 0
        for i in range(len(columns)):
            if columns[i] is not None:
                self.pages[i].update(columns[i], offset)
                
                # append a 1 in the location where the column has been updated
                (1 << i) | updated_schema
        self.schema_encodings[offset] = updated_schema


        # Need a new RID for tail page
        # Need indirection for base page i.e. we'll need to update the base page's RID each time
        # If a previous tail page existed, its indirection must be updated as well
        


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