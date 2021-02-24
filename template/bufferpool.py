from template.config import *
from collections import deque

class Bufferpool:
    def __init__(self):

        # eviction policy currently will be least recently used
        
        # here, data = page_set and meta  data
        self.pages_mem_mapping = {} # {[table_name, set_index, set_type]} : data, num_columns, block_start_index}
        self.dirty_pages = set() # set of [table_name, rid] that are dirty and need to be written to disk before eviction
        self.pinned_pages = {} # dict of [table_name, rid] : pin_num indicating if the current RID is pinned. By default, this is zero
        self.tables = None # dict of {table name : pointer } for easy communication

        # using a LRU setup, 
        self.lru_enforcement = deque(maxlen=MAX_PAGES_IN_BUFFER) # dequeue of [table_name, rid]
        # consistency is important, pop_left to remove, and append to insert to the right
        # if something is re-referenced, we will remove and then append again

    


    def get_page_set(self, table_name, num_columns, disk, rid, set_type, block_start_index):



        # if data is not in memory
        if self.pages_mem_mapping[table_name, rid] == None:
            data = self.__load_page_set(disk, num_columns, set_type, block_start_index)
            # append datapoint to lru_enforcement and add to current page tiles
            self.lru_enforcement.append([table_name, rid])
            self.pages_mem_mapping[table_name, rid] = data, num_columns

        else: # pull data from current mem
            data = self.pages_mem_mapping[table_name, rid]
            # reset its position in lru
            self.lru_enforcement.remove([table_name, rid])
            self.lru_enforcement.append([table_name, rid])

        # pin page, for its in use
        self.pin_page_set(table_name, rid)
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

        return data

    # kick out least recently used page from queue
    def __ensure_buffer_pool_can_fit_new_data(self, num_columns):
        # we must be careful here, due to the fact that dequeue will throw a max size exception. I may need to catch it somewhere
        while (len(self.lru_enforcement) + num_columns + META_DATA_PAGES > MAX_PAGES_IN_BUFFER):
            table_name, rid = self.lru_enforcement.pop_left()
            
            # page is currently pinned and we cannot evict it
            if self.pinned_pages[table_name, rid] != None:
                # page is currently in use and cannot be evicted
                # add it back to the queue
                self.lru_enforcement.append(table_name, rid)

            # page can be evicted, remove    
            else: 
                self.__evict_page_set(table_name, rid)

    # evaluate if page is dirty then remove any traces
    def __evict_page_set(self, table_name, rid):
        if self.__is_dirty(table_name, rid):
            self.__write_to_disk(rid, table_name)

        # remove entry from dictonary
        self.pages_mem_mapping.pop([table_name, rid])

    # allocate new space for a page_set
    # assume meta data is packed
    def get_new_free_mem_space(self, table_name, rid, num_columns, data):
        self.__ensure_buffer_pool_can_fit_new_data(num_columns)
        
        # add data to the bufferpool and LRU queue
        self.pages_mem_mapping[table_name, rid] = data, num_columns
        self.lru_enforcement.append(table_name, rid)
        
        self.pin_page_set(table_name, rid)

        # also mark table_name, RID as being in use right now

    def pin_page_set(self, table_name, rid):
        # start at zero and build up
        if self.pinnned_pages[table_name, rid] == None:
            # add pair with 1 to indicate we just started pinning
            self.pinned_pages[table_name, rid] = 1
        else: 
            # increase the amount of users using the page, for M3 safe keeping
            self.pinned_pages[table_name, rid] = self.pinned_pages[table_name, rid] + 1

        
    def __is_dirty(self, table_name, rid):
        return table_name, rid in self.dirty_pages 
    
    # called from table when the ref counter needs to be dec/removed
    def unpin_page(self):
        pass


    @staticmethod
    def pack_data(page_set, metadata):
        pass

    
    @staticmethod
    def unpack_data(data):
        # last 3 pages are meta data information, do something to figure this out
        pass


    # To write to disk, pack like how I'm reading it, where last 3 pages are meta data
    def __write_to_disk(self, table_name, rid):
        # write to the disk
        pass

    # for M3, write a handler that'll check to see if every page is evicted and if so, block until one is free








    

