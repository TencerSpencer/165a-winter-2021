from template.index import Index
from template.pageRange import PageRange
from template.config import *
import datetime, threading, time, copy


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class MergeHandler:
    def __init__(self):
        # setup merge variables
        self.next_time_to_call = time.time() # prepare consistent time callback
        self.timer_interval = .5 # every fourth a second, the timer will trigger, used as an example
        self.thread_in_crit_section = False
        self.thread_stopped = False

        # prevent timer from executing if we are performing an update with a mutex
        # https://docs.python.org/3/library/threading.html#threading.Lock.acquire
        self.dict_mutex = threading.Lock()

        # track RIDs to merge inside a dict, this implementation has yet to be setup per page range
        # {base rid : }
        self.rids_to_merge = {}

        # handlers for epoch based deallocation [(RID queried : query start time)]
        self.query_queue = [] 

        # dict of base RID to offset in old copy of base record
        self.outdated_offsets = {} #{base RID : old offset}

        # handler for deallocation queue (pages to be deallocated)
        self.dealloc_queue = []

        # list of sequential merge end times, queue emulation
        self.merge_end = []

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
        self.keys = {}  # key-value pairs { key : rid }
        self.num_columns = num_columns
        self.page_directory = {}  # key-value pairs { rid : (page range index, base page set index) }
        self.index = Index(self)
        self.next_base_rid = START_RID
        self.next_tail_rid = START_RID
        self.page_range_array = []
        self.merge_handler = MergeHandler()

        # when we start the timer, any class variables assigned after may not be captured, so do it at the end
        self.merge_handler.thread = threading.Timer(self.merge_handler.next_time_to_call - time.time(), self.__merge_callback)
        self.merge_handler.thread.start()



    # for help with background process operation and to ensure timer is consistent
    # https://stackoverflow.com/questions/8600161/executing-periodic-actions-in-python
    def __merge_callback(self): 

        if self.merge_handler.thread_stopped == False:
            
            # call _merge
            self.merge_handler.thread_in_crit_section = True
            self.__merge()
            self.merge_handler.thread_in_crit_section = False
            self.merge_handler.thread = threading.Timer(self.merge_handler.next_time_to_call - time.time(), self.__merge_callback)
            self.merge_handler.next_time_to_call = self.merge_handler.next_time_to_call + self.merge_handler.timer_interval
            self.merge_handler.thread.start() 

    def shut_down_timer(self): # if constant printing is occurring, this wont properly stop
        
        if self.merge_handler.thread_in_crit_section:
            # kill a certain way
            self.merge_handler.thread_stopped = True
            self.merge_handler.thread.join()
        else:
            self.merge_handler.thread_stopped = True
            self.merge_handler.thread.cancel()
            # use this to halt timer, but does not halt thread as per documentation

    # this implementation will first be in mem
    def __merge(self):
        
        if len(self.merge_handler.rids_to_merge) != 0:         

            # dictonaries will error out if their size changes during copy, so use a mutex for copying
            self.merge_handler.dict_mutex.acquire()
            rid_dict = copy.deepcopy(self.merge_handler.rids_to_merge)
            self.merge_handler.rids_to_merge.clear()
            self.merge_handler.dict_mutex.release()

            for base_rid in rid_dict.keys():

                # todo: need to allocate new space
                page_range_index = self.page_directory[base_rid][0]
                cur_page_range = self.page_range_array[page_range_index]

                tail_rid = rid_dict[base_rid]

                data = cur_page_range.get_record_with_tail(base_rid, tail_rid, [1,1,1,1,1])
                
                new_base_record = data

                # Write new section


                # For now, I am just going to write this new base record over the previous i.e.
                #self.__overwite_previous_base_record(base_rid, new_base_record)

                # next, figure out merge deallocation   
            print("merge complete")
            self.merge_handler.merge_end.append(time.time())

    # deallocate base pages when all active queries started after merge time
    def __dealloc(self):
        most_recent_merge = self.merge_handler.merge_end.pop(0)
        
        # peek first active query, if start time > most recent merge, deallocate base page
        if self.query_queue[0][1] > most_recent_merge:
            dealloc_rid = self.query_queue[0][0]
            self.merge_handler.dealloc_queue.append()
        

    # THIS FUNCTION WILL BE REMOVED AND IS ONLY USED TO TEST MERGES PURELY IN MEM
    # force schema to be zero
    def __overwite_previous_base_record(self, base_rid, new_data):
        page_range_index = self.page_directory[base_rid][0]
        cur_page_range = self.page_range_array[page_range_index]
        old_offset = cur_page_range.overwrite_previous_base_record(base_rid, new_data)
        self.merge_handler.outdated_offsets[base_rid] = old_offset



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

        result = self.page_range_array[page_range_index].update_record(base_rid, tail_rid, columns)

        
        self.merge_handler.dict_mutex.acquire()
        # append to base RID to a set of RIDs to merge, only do so after update is done, but why does it seem like this is running first?
        self.merge_handler.rids_to_merge[base_rid] = tail_rid 
        self.merge_handler.dict_mutex.release()

        return result

    def select_record(self, key, query_columns):
        if key not in self.keys:
            return False

        rid = self.keys[key]
        page_range_index = self.page_directory[rid][0]

        # record time when query starts for given RID
        self.merge_handler.query_queue.append((rid, time.time()))

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
