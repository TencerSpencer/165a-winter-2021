from template.config import *
from collections import deque

class Bufferpool:
    def __init__(self):

        # eviction policy currently will be least recently used
        
        # here, data = page_set and meta  data
        self.pages_mem_mapping = {} # {[table_name, rid]} : data, num_columns}
        self.num_pages_in_mem = 0 # whenever we load in a page, increment this, whenever we unload a page, decrement this
        self.dirty_pages = set() # set of rids that are dirty and need to be written to disk before eviction
        self.pinned_pages = {} # dict of rid : pin_num indicating if the current RID is pinned. By default, this is zero
        self.tables = None # dict of {table name : pointer } for easy communication

        # using a LRU setup, 
        self.lru_enforcement = deque() # dict of {[table_name, rid] : data_offset}
        # consistency is important, pop_left to remove, and append to insert to the right
        # if something is re-referenced, we will remove and then append again

        


    def get_page_set(self, table_name, num_columns, disk, rid, set_type, block_start_index):

        if self.pages_mem_mapping[table_name, rid] == None:
            data, offset = self.__load_page_set(disk, num_columns, set_type, block_start_index)
            # append datapoint to lru_enforcement
            # self.lru_enforcement

        
        else:
            # load data, segment it
            data = self.pages_mem_mapping[table_name, rid]

        # ignore pinning/unpinning
        # segment data, then mark it as pinned because it is in use
        page_set, meta_data = self.__unpack_data(data)
        return page_set, meta_data

    def __load_page_set(self, disk, num_columns, set_type, block_start_index):
        # we must first evict pages before we load them in
        self.__ensure_buffer_pool_can_fit_new_data(num_columns)

        data = None
        
        if set_type == BASE_RID_TYPE:
            data = disk.read_base_page_set(block_start_index)

        elif set_type == TAIL_RID_TYPE: # we have a tail to bring in,
            data = disk.read_tail_page_set(block_start_index)

        # TODO: put data into memory and then return, data should also have its metadata properly formatted

        page_set, metadata = self.__unpack_page_set_and_metadata(data)

        pass # should return this tuple, but not sure what else should be done first, should also return an offset

    def __ensure_buffer_pool_can_fit_new_data(self, num_columns):
        pass
       # while (self.num_pages_in_mem + num_columns + META_DATA_PAGES > MAX_PAGES_IN_BUFFER):
            # evict a base_page_set and retry

    def __unload_page_set(self):
        pass


    def __get_new_free_mem_space(self):
        pass

    def pack_page_set_and_metadata(self):
        pass

    def __unpack_data(self, data):
        pass



    def __is_dirty(self, rid):
        return rid in self.dirty_pages 

    # called after determing that this rid is to be evicted
    def __evict_page_set(self, rid):
        if self.__is_dirty(rid):
            self.__write_to_disk(rid)
            pass

        # afterwards, evict page i.e. remove it from anything that it was previously bound to

    # To write to disk, pack like how I'm reading it, where last 3 pages are meta data
    def __write_to_disk(self, rid):
        # write to the disk
        pass

    def pin_page(self, rid):
        pass
        








    

