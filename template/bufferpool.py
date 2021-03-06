from template.bufferpool_config import *
from template.lock_manager_config import *
from collections import deque
from template.pageSet import PageSet
from template.tools import *


class Bufferpool:
    def __init__(self):

        # eviction policy currently will be least recently used

        # here, data = page_set and meta  data
        self.pages_mem_mapping = {}  # {(table_name, page_range_index, page_set_index, set_type)} : data, num_columns }
        self.dirty_page_sets = set()  # set of (table_name, page_range_index, page_set_index, set_type) that are dirty and need to be written to disk before eviction
        self.pinned_page_sets = {}  # dict of (table_name, page_range_index, page_set_index, set_type) : pin_num indicating if the current RID is pinned. By default, this is zero
        self.tables = {}  # dict of {table name : pointer } for easy communication

        # using a LRU setup,
        self.lru_enforcement = deque(maxlen=MAX_PAGES_IN_BUFFER)  # dequeue of [table_name, page_range_index, page_set]
        # consistency is important, pop_left to remove, and append to insert to the right
        # if something is re-referenced, we will remove and then append again

    # DUE TO GIL, LRU lock may not be necessary, however, I use it to preserve order
    def get_page_set(self, table_name, num_columns, disk, page_range_index, page_set_index, set_type,
                     block_start_index):

        # if data is not in memory
        LOCK_MANAGER.latches[PAGES_MEM_MAP_BLOCK].acquire()
        
        if not self.pages_mem_mapping.get((table_name, page_range_index, page_set_index, set_type)):
           
            data = self.__load_page_set(disk, num_columns, set_type, block_start_index)
            page_set, rids, times, schema, indir, indir_t = self.unpack_disk_data(data)
            # append datapoint to lru_enforcement and add to current page tiles
            
            LOCK_MANAGER.latches[LRU_ENFORCEMENT].acquire()
            self.lru_enforcement.append((table_name, page_range_index, page_set_index, set_type))
            LOCK_MANAGER.latches[LRU_ENFORCEMENT].release()
            
            # storing data with meta data packed in bufferpool
            self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)] = data, num_columns

        else:  # pull data from current mem
            data, _ = self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)]
            page_set, rids, times, schema, indir, indir_t = self.unpack_disk_data(data)
            # reset its position in lru
            LOCK_MANAGER.latches[LRU_ENFORCEMENT].acquire()
            self.lru_enforcement.remove((table_name, page_range_index, page_set_index, set_type))
            self.lru_enforcement.append((table_name, page_range_index, page_set_index, set_type))
            LOCK_MANAGER.latches[LRU_ENFORCEMENT].release()


        LOCK_MANAGER.latches[PAGES_MEM_MAP_BLOCK].release()

        # pin page, for its in use
        #self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] = 1
        # segment data, then mark it as pinned because it is in use
        return page_set, rids, times, schema, indir, indir_t

    def __load_page_set(self, disk, num_columns, set_type, block_start_index):
        
        # This may not need to be locked due to the fact that we use a latch on ensure_buffer_pool_can_fit

        # LOCK_MANAGER.latches[DISK_ACCESS].acquire()
        # we must first evict pages before we load them in
        self.__ensure_buffer_pool_can_fit_new_data(num_columns)

        data = None

        if set_type == BASE_RID_TYPE:
            data = disk.read_base_page_set(block_start_index)

        elif set_type == TAIL_RID_TYPE:  # we have a tail to bring in,
            data = disk.read_tail_page_set(block_start_index)

        # LOCK_MANAGER.latches[DISK_ACCESS].release()
        return data

    # kick out least recently used page from queue
    def __ensure_buffer_pool_can_fit_new_data(self, num_columns):
        LOCK_MANAGER.latches[BUFFER_POOL_SPACE].acquire()
        # we must be careful here, due to the fact that dequeue will throw a max size exception. I may need to catch it somewhere
        while len(self.lru_enforcement) + num_columns + META_DATA_PAGES > MAX_PAGES_IN_BUFFER:
            LOCK_MANAGER.latches[LRU_ENFORCEMENT].acquire()
            table_name, page_range_index, page_set_index, set_type = self.lru_enforcement.popleft()
            LOCK_MANAGER.latches[LRU_ENFORCEMENT].release()

            # page is currently pinned and we cannot evict it
            if self.pinned_page_sets.get((table_name, page_range_index, page_set_index, set_type)) != None:
                # page is currently in use and cannot be evicted
                LOCK_MANAGER.latches[LRU_ENFORCEMENT].acquire()
                # add it back to the queue
                self.lru_enforcement.append((table_name, page_range_index, page_set_index, set_type))
                LOCK_MANAGER.latches[LRU_ENFORCEMENT].release()

            # page can be evicted, remove
            else:
                self.__evict_page_set(table_name, page_range_index, page_set_index, set_type)

        LOCK_MANAGER.latches[BUFFER_POOL_SPACE].release()

    # evaluate if page is dirty then remove any traces
    def __evict_page_set(self, table_name, page_range_index, page_set_index, set_type):
        
        # this mutex may possibly be shortened
        LOCK_MANAGER.latches[PAGE_SET_EVICTION].acquire()
        
        if self.__is_dirty(table_name, page_range_index, page_set_index, set_type):
            table = self.tables[table_name]
            table.disk.next_base_rid = table.next_base_rid
            table.disk.next_tail_rid = table.next_tail_rid
            table.disk.write_file_info()
            table.disk.write_key_directory_set(table.keys, table.brid_to_trid, table.brid_block_start,
                                               table.trid_block_start)
            meta = self.tables[table_name].get_meta_data(page_range_index, page_set_index, set_type)
            self.__write_to_disk(table_name, page_range_index, page_set_index, set_type, meta)

        # remove entry from dictionary
        self.pages_mem_mapping.pop((table_name, page_range_index, page_set_index, set_type))
        self.dirty_page_sets.discard((table_name, page_range_index, page_set_index, set_type))
        
        
        # This lock does not need to be extended because multiple calls to this function from the same page_set
        # will fail once the dirt page set is discarded
        LOCK_MANAGER.latches[PAGE_SET_EVICTION].release()

        # get the current table 
        current_table = self.tables[table_name]
        current_page_range = self.tables[table_name].page_ranges[page_range_index]

        try:
            current_table.merge_handler.full_base_page_sets.remove((page_range_index, page_set_index))
        except ValueError:
            pass


        # remove its base rids from the page directory
        if (set_type == 0):

            LOCK_MANAGER.latches[PAGE_DIR].acquire()
            rids = [k for k, v in current_table.page_directory.items() if v[0] == page_range_index and v[1] == page_set_index]
            for i in range(len(rids)):
                offset = current_page_range.base_rids[rids[i]][1]
                current_table.page_directory.pop(rids[i])
                current_page_range.base_schema_encodings.pop(offset)
                current_page_range.base_indirections.pop(offset)
                current_page_range.base_timestamps.pop(offset)
                current_page_range.base_rids.pop(rids[i])
            current_page_range.base_page_sets.pop(page_set_index)
            LOCK_MANAGER.latches[PAGE_DIR].release()
                
        # remove tail rids from respective base page set from page range area
        if (set_type == 1):
            rids = [k for k, v in current_page_range.tail_rids.items() if v[0] == page_set_index]
            for i in range(len(rids)):
                offset = current_page_range.tail_rids[rids[i]][1]
                current_page_range.tail_schema_encodings.pop(offset)
                current_page_range.tail_indirections.pop(offset)
                current_page_range.tail_timestamps.pop(offset)
                current_page_range.tail_rids.pop(rids[i])
            current_page_range.tail_page_sets.pop(page_set_index)



    # self.tail_rids = {}  # key-value pairs: { rid : (page set index, offset) }
    # allocate new space for a page_set
    # assume meta data is packed
    def get_new_free_mem_space(self, table_name, page_range_index, page_set_index, num_columns, set_type):
        self.__ensure_buffer_pool_can_fit_new_data(num_columns)

        data = PageSet(num_columns + META_DATA_PAGES)
        page_set, _, _, _, _, _ = self.unpack_new_data(data)

        # add data to the bufferpool and LRU queue
        self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)] = data, num_columns
        LOCK_MANAGER.latches[LRU_ENFORCEMENT].acquire()
        self.lru_enforcement.append((table_name, page_range_index, page_set_index, set_type))
        LOCK_MANAGER.latches[LRU_ENFORCEMENT].release()
        #self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] = 1
        return page_set

        # also mark table_name, page_set_index as being in use right now

    def pin_page_set(self, table_name, page_range_index, page_set_index, set_type):
        LOCK_MANAGER.latches[PIN_PAGE_SET].acquire()
        # start at zero and build up
        if not self.pinned_page_sets.get((table_name, page_range_index, page_set_index, set_type)):
             # add pair with 1 to indicate we just started pinning
            self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] = 1
        else:
            # increase the amount of users using the page, for M3 safe keeping
            self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] += 1
        LOCK_MANAGER.latches[PIN_PAGE_SET].release()

    def __is_dirty(self, table_name, page_range_index, page_set_index, set_type):
        return (table_name, page_range_index, page_set_index, set_type) in self.dirty_page_sets

    # called from table when the ref counter needs to be dec/removed
    def unpin_page_set(self, table_name, page_range_index, page_set_index, set_type):
        LOCK_MANAGER.latches[UNPIN_PAGE_SET].acquire()
        if self.pinned_page_sets.get((table_name, page_range_index, page_set_index, set_type)) == None:
            LOCK_MANAGER.latches[UNPIN_PAGE_SET].release()
            return
        
        self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] -= 1

        # if the page is no longer in use, remove it from the mapping, in m3 we may decide to keep it
        if self.pinned_page_sets.get((table_name, page_range_index, page_set_index, set_type)) == 0:
            self.pinned_page_sets.pop((table_name, page_range_index, page_set_index, set_type))
        LOCK_MANAGER.latches[UNPIN_PAGE_SET].release()

    # write dirty data to disk
    # no locks needed because this will run on the main thread after all transactions have finished
    def flush_buffer_pool(self):
        
        for table in self.tables.values():
            table.disk.next_base_rid = table.next_base_rid
            table.disk.next_tail_rid = table.next_tail_rid
            table.disk.write_file_info()
            table.disk.write_key_directory_set(table.keys, table.brid_to_trid, table.brid_block_start,
                                               table.trid_block_start)

        self.dirty_page_sets = sorted(self.dirty_page_sets)

        for (table_name, page_range_index, page_set, set_type) in self.dirty_page_sets:
            table = self.tables[table_name]
            meta_data = table.get_meta_data(page_range_index, page_set, set_type)
            self.__write_to_disk(table_name, page_range_index, page_set, set_type, meta_data)

        self.dirty_page_sets.clear()
        self.pages_mem_mapping.clear()
        self.lru_enforcement.clear()
        self.tables.clear()
        self.pinned_page_sets.clear()
        return

    # allow a table to mark pagesets as dirty
    def mark_as_dirty(self, table_name, page_range_index, page_set_index, set_type):
        self.dirty_page_sets.add((table_name, page_range_index, page_set_index, set_type))

    def unpack_disk_data(self, data):
        # last 3 pages are meta data information, do something to figure this out
        num_columns = len(data.pages) - META_DATA_PAGES
        page_set = PageSet(num_columns)
        for i in range(num_columns):
            page_set.pages[i] = data.pages[i]

        rids = []
        timestamps = []
        schema = []
        indirections = []
        indirection_types = []

        meta_start = num_columns
        rids_data = data.pages[meta_start]
        timestamps_data = data.pages[meta_start + 1]
        schema_data = data.pages[meta_start + 2]
        indirections_data = data.pages[meta_start + 3]
        indirection_types_data = data.pages[meta_start + 4]

        for i in range(RECORDS_PER_PAGE):
            rids.append(int.from_bytes(rids_data.data[(i * 8):(i * 8) + 8], "little"))
            timestamps.append(int.from_bytes(timestamps_data.data[(i * 8):(i * 8) + 8], "little"))
            schema.append(int.from_bytes(schema_data.data[(i * 8):(i * 8) + 8], "little"))
            indirections.append(int.from_bytes(indirections_data.data[(i * 8):(i * 8) + 8], "little"))
            indirection_types.append(int.from_bytes(indirection_types_data.data[(i * 8):(i * 8) + 8], "little"))

        cutoff = -1
        for i in range(RECORDS_PER_PAGE):
            if indirection_types[i] == 4:
                cutoff = i
                break

        if cutoff != -1:
            rids = rids[0:cutoff]
            timestamps = timestamps[0:cutoff]
            schema = schema[0:cutoff]
            indirections = indirections[0:cutoff]
            indirection_types = indirection_types[0:cutoff]

        for i in range(len(indirection_types)):
            if indirection_types[i] == 0:
                indirections[i] = None
                indirection_types[i] = None

        for page in page_set.pages:
            page.num_records = len(rids)

        return page_set, rids, timestamps, schema, indirections, indirection_types

    def unpack_new_data(self, data):
        # last 3 pages are meta data information, do something to figure this out
        num_columns = len(data.pages) - META_DATA_PAGES
        page_set = PageSet(num_columns)
        for i in range(num_columns):
            page_set.pages[i] = data.pages[i]

        rids = []
        timestamps = []
        schema = []
        indirections = []
        indirection_types = []

        meta_start = num_columns
        rids_data = data.pages[meta_start]
        timestamps_data = data.pages[meta_start + 1]
        schema_data = data.pages[meta_start + 2]
        indirections_data = data.pages[meta_start + 3]
        indirection_types_data = data.pages[meta_start + 4]

        for i in range(RECORDS_PER_PAGE):
            rids.append(int.from_bytes(rids_data.data[(i * 8):(i * 8) + 8], "little"))
            timestamps.append(int.from_bytes(timestamps_data.data[(i * 8):(i * 8) + 8], "little"))
            schema.append(int.from_bytes(schema_data.data[(i * 8):(i * 8) + 8], "little"))
            indirections.append(int.from_bytes(indirections_data.data[(i * 8):(i * 8) + 8], "little"))
            indirection_types.append(int.from_bytes(indirection_types_data.data[(i * 8):(i * 8) + 8], "little"))

        return page_set, rids, timestamps, schema, indirections, indirection_types

    # To write to disk, pack like how I'm reading it, where last 3 pages are meta data
    def __write_to_disk(self, table_name, page_range_index, page_set_index, set_type, meta):
        
        # This will now lock at the disk level instead, in order to increase speeds
        # LOCK_MANAGER.latches[DISK_ACCESS].acquire()

        data, num_columns = self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)]
        self.__update_meta_data(data, meta)
        table = self.tables[table_name]
        disk = self.tables[table_name].disk
        if set_type is BASE_RID_TYPE:
            
            
            LOCK_MANAGER.latches[PAGE_DIR].acquire()
            rid = [k for k, v in table.page_directory.items() if v[0] == page_range_index and v[1] == page_set_index]
            LOCK_MANAGER.latches[PAGE_DIR].release()
            block_start_index = table.brid_block_start[rid[0]]  # just need a single rid that has the block start index
            disk.write_base_page_set(data, block_start_index)
        else:
            LOCK_MANAGER.latches[PAGE_DIR].acquire()
            rid = [k for k, v in table.page_ranges[page_range_index].tail_rids.items() if v[0] == page_set_index]
            LOCK_MANAGER.latches[PAGE_DIR].release()
            block_start_index = table.trid_block_start[rid[0]]  # just need a single rid that has the block start index
            disk.write_tail_page_set(data, block_start_index)
        
        # LOCK_MANAGER.latches[DISK_ACCESS].release()

    def __update_meta_data(self, data, meta):
        rids = meta[0]
        timestamps = meta[1]
        schema = meta[2]
        indirections = meta[3]

        indirection_types = meta[4]

        rids_data = []
        timestamps_data = []
        schema_data = []
        indirections_data = []
        indirection_types_data = []

        for num in rids:
            for byte in int.to_bytes(num, length=8, byteorder="little"):
                rids_data.append(byte)
        rids_ba = bytearray(rids_data)
        pad_byte_array(rids_ba)

        for num in timestamps:
            for byte in int.to_bytes(num, length=8, byteorder="little"):
                timestamps_data.append(byte)
        timestamps_ba = bytearray(timestamps_data)
        pad_byte_array(timestamps_ba)

        for num in schema:
            for byte in int.to_bytes(num, length=8, byteorder="little"):
                schema_data.append(byte)
        schema_ba = bytearray(schema_data)
        pad_byte_array(schema_ba)

        for num in indirections:
            if num:  # since indirection can be none, check if its none
                for byte in int.to_bytes(num, length=8, byteorder="little"):
                    indirections_data.append(byte)
            else:
                indirections_data.extend([0] * 8)
        indirections_ba = bytearray(indirections_data)
        pad_byte_array(indirections_ba)

        for num in indirection_types:
            if num:  # since indirection type can be none, check if its none
                for byte in int.to_bytes(num, length=8, byteorder="little"):
                    indirection_types_data.append(byte)
            else:
                indirection_types_data.extend([0] * 8)
        indirection_types_ba = bytearray(indirection_types_data)
        pad_byte_array(indirection_types_ba, 4)

        meta_start = len(data.pages) - META_DATA_PAGES
        data.pages[meta_start].data = rids_ba
        data.pages[meta_start + 1].data = timestamps_ba
        data.pages[meta_start + 2].data = schema_ba
        data.pages[meta_start + 3].data = indirections_ba
        data.pages[meta_start + 4].data = indirection_types_ba
