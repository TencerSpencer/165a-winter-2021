from template.index import Index
from template.pageRange import PageRange
from template.config import *
import datetime, threading, time, copy


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key  # This is the index of the table key in columns that are sent in
        self.keys = {}  # key-value pairs { key : rid }
        self.num_columns = num_columns
        self.page_directory = {}  # key-value pairs { rid : (page range index, base page set index) }
        self.index = Index(self)
        self.next_base_rid = START_RID
        self.next_tail_rid = START_RID
        self.page_range_array = []
        
        # setup merge variables
        self.next_time_to_call = time.time() # prepare consistent time callback
        self.timer_interval = 0.5 # every half a second, the timer will trigger, used as an example
        self.thread_in_crit_section = False
        self.thread_stopped = False
        self.rids_to_merge = set()
        # initially, call the thread instantly
        self.thread = self.thread = threading.Timer(self.next_time_to_call - time.time(), self.__merge_callback)
        self.thread.start()


    # for help with background process operation and to ensure timer is consistent
    # https://stackoverflow.com/questions/8600161/executing-periodic-actions-in-python
    def __merge_callback(self): 

        if self.thread_stopped == False:
            print(datetime.datetime.now())
            # call _merge
            self.__merge()
            self.thread_in_crit_section = False
            self.thread = threading.Timer(self.next_time_to_call - time.time(), self.__merge_callback)
            self.next_time_to_call = self.next_time_to_call + self.timer_interval
            self.thread.start() 

    def shut_down_timer(self): # if constant printing is occurring, this wont properly stop
        if self.thread_in_crit_section:
            # kill a certain way
            # self.thread.join()
            self.thread_stopped = True
        else:
            self.thread.cancel()
             # use this to halt timer, but does not halt thread as per documentation

    # this implementation will first be in mem
    def __merge(self):
        self.thread_in_crit_section = True
        if len(self.rids_to_merge) == 0:
            print("no merge necessary, table is up to date")
            # no other action needed here
        else:
            # here, we can do length or duration checks
            # if we're doing a duration check, we need to check how much time has passed
            print("merge necessary")
                
            # deep copy set then clear original to prevent contention
            rid_set = copy.deepcopy(self.rids_to_merge)
            self.rids_to_merge.clear()
            for base_rid in rid_set:
                print(base_rid)
                       
                # 1. bring original base page to mem and copy it over
            
                # base_and_tail = read(base_rid) # should return the base and tail 

                # 2. make a copy of the original base record to ensure mutual exclusion
                # 3. get the most up to date tail for the base record and put it in mem
                # 4. append data from tail to base record
                # 5. allocate space to new base record on page with available space
                # 6. do we leave the rest of the tail untouched and just deallocate it from memory?
            
                # some of these steps can be done in their own for loop to prevent lengthy block
                # 7. keep original base page in mem/intact until no queries are being ran on it i.e. update/select
                # 8. once original is no longer needed, update page directory to point to newe base record
                # 9. do we deallocate the original base page here? -> free up the storage?
                # or, do we assign a new RID to the new base, and free up the RID of the previous?
                # 10. do this for each baseRID in the set



    def insert_record(self, *columns):
        key_col = self.key
        new_rid = self.__get_next_base_rid()
        col_list = list(columns)
        key = col_list[key_col]

        next_free_page_range_index = self.__get_next_available_page_range()
        next_free_base_page_set_index = self.page_range_array[next_free_page_range_index].get_next_free_base_page_set()

        # set key and rid mappings
        self.keys[key] = new_rid
        self.page_directory[new_rid] = (next_free_page_range_index, next_free_base_page_set_index)

        # continue with inserting the record here
        curr_page_range = self.page_range_array[next_free_page_range_index]
        return curr_page_range.add_record(new_rid, col_list)

    def update_record(self, key, *columns):
        base_rid = self.keys[key]
        page_range_index = self.page_directory[base_rid][0]
        tail_rid = self.__get_next_tail_rid()
        
        # append to base RID to a set of RIDs to merge
        self.rids_to_merge.add(base_rid)

        return self.page_range_array[page_range_index].update_record(base_rid, tail_rid, columns)

    def select_record(self, key, query_columns):
        if key not in self.keys:
            return False

        rid = self.keys[key]
        page_range_index = self.page_directory[rid][0]
        cur_page_range = self.page_range_array[page_range_index]
        data = cur_page_range.get_record(rid, query_columns)
        return rid, data

    def remove_record(self, key):
        if key in self.keys:
            self.keys.pop(key)
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
        if len(self.page_range_array) == 0:
            # no current page range, build one for starters and append to list
            new_page_range = PageRange(self.num_columns)
            self.page_range_array.append(new_page_range)
            # our index will be our only page range
            return 0

        # note, I assume that base records will be removed at some point, so I choose to loop through each
        for i in range(len(self.page_range_array)):
            if not self.page_range_array[i].is_full():
                return i

        # if we reached here, then all our current page ranges are full. As such, build a new one
        new_page_range = PageRange(self.num_columns)
        self.page_range_array.append(new_page_range)

        # length returns an index + 1, so subtract one to compensate
        return len(self.page_range_array) - 1
