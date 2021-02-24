from template.config import *

class Bufferpool:
    def __init__(self):
        
        self.pages = []
        self.num_pages_in_mem = 0 # whenever we load in a page, increment this, whenever we unload a page, decrement this
        self.dirty_pages = set() # set of rids that are dirty and need to be written to disk before eviction
        self.pinned_pages = {} # dict of rid : pin_num indicating if the current RID is pinned. By default, this is zero


        # dict of key tuple being table name and RID, and value will be page set reference for where it is in pages and number of columns, possibly
        


    def get_page_set(disk, rid, set_type, block_start_index):

        # if RID is in mem, return proper information, may involve checking the dictonary
        # else:
            return self.__load_page_set(disk, num_columns, set_type, block_start_index)

    def __load_page_set(disk, num_columns, set_type, block_start_index):
        if num_pages_in_mem + num_columns > MAX_PAGES_IN_BUFFER:
            # we must first evict pages before we load them in
        
        data = None
        else: # figure out if it's a tail or not using set_type
            if set_type == BASE_RID_TYPE:
                data = disk.read_base_page_set(block_start_index)

            elif set_type == TAIL_RID_TYPE: # we have a tail to bring in,
                data = disk.read_tail_page_set(block_start_index)

        # TODO: put data into memory and then return
        return data



    def __unload_page_set():
        pass


    def __get_new_free_mem_space():



    def __is_dirty(rid):
        return rid in dirty_pages 

    # called after determing that this rid is to be evicted
    def __evict_page_set(rid):
        if self.__is_dirty(rid):
            self.__write_to_disk(rid)
            pass

        # afterwards, evict page i.e. remove it from anything that it was previously bound to

    def __write_to_disk(rid):
        # write to the disk
        pass

    def pin_page(rid):
        pass
        








    

