from template.config import *

BASE_RID_TYPE = 0
TAIL_RID_TYPE = 1

class Bufferpool:
    def __init__(self):
        
        self.pages = []
        self.num_pages_in_mem = 0 # whenever we load in a page, increment this, whenever we unload a page, decrement this
        self.dirty_pages = set() # set of rids that are dirty and need to be written to disk before eviction
        self.pinned_pages = {} # dict of rid : pin_num indicating if the current RID is pinned. By default, this is zero
        

    def __load_page_set(num_columns):
        if num_pages_in_mem + num_columns > MAX_PAGES_IN_BUFFER:
            # we must first evict pages before we load them in
        else:
            # load in the page set
        pass

    '''
    def __load_page():
        if num_pages_in_mem + 1 > MAX_PAGES_IN_BUFFER:
            # evict one page before we load them in
        else:
            # load in the page
        pass
    '''

    def __is_dirty(rid):
        return rid in dirty_pages 

    # called after determing that this rid is to be evicted
    def __evict_page_set(rid):
        if self.__is_dirty(rid):
            self.__write_to_disk(rid)
            pass

        # afterwards, evict page

    def __write_to_disk(rid):
        # write to the disk
        pass

    def pin_page(rid):
        pass
        








    

