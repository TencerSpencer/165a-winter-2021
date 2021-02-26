from template.bufferpool_config import *
from collections import deque
from template.pageSet import PageSet
from template.tools import *

class Bufferpool:
    def __init__(self):

        # eviction policy currently will be least recently used

        # here, data = page_set and meta  data
        self.pages_mem_mapping = {}  # {(table_name, page_range_index, page_set_index, set_type)} : data, num_columns, block_start_index}
        self.dirty_page_sets = set()  # set of (table_name, page_range_index, page_set_index, set_type) that are dirty and need to be written to disk before eviction
        self.pinned_page_sets = {}  # dict of (table_name, page_range_index, page_set_index, set_type) : pin_num indicating if the current RID is pinned. By default, this is zero
        self.tables = {}  # dict of {table name : pointer } for easy communication

        # using a LRU setup,
        self.lru_enforcement = deque(maxlen=MAX_PAGES_IN_BUFFER)  # dequeue of [table_name, page_range_index, page_set]
        # consistency is important, pop_left to remove, and append to insert to the right
        # if something is re-referenced, we will remove and then append again

    def get_page_set(self, table_name, num_columns, disk, page_range_index, page_set_index, set_type, block_start_index):

        # if data is not in memory
        if self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)] is None:
            data = self.__load_page_set(disk, num_columns, set_type, block_start_index)
            # append datapoint to lru_enforcement and add to current page tiles
            self.lru_enforcement.append((table_name, page_range_index, page_set_index))
            # storing data with meta data packed in bufferpool
            self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)] = data, num_columns

        else:  # pull data from current mem
            data = self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)]
            # reset its position in lru
            self.lru_enforcement.remove((table_name, page_range_index, page_set_index))
            self.lru_enforcement.append((table_name, page_range_index, page_set_index))

        # pin page, for its in use
        self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] += 1
        # segment data, then mark it as pinned because it is in use
        return data

    def __load_page_set(self, disk, num_columns, set_type, block_start_index):
        # we must first evict pages before we load them in
        self.__ensure_buffer_pool_can_fit_new_data(num_columns)

        data = None

        if set_type == BASE_RID_TYPE:
            data = disk.read_base_page_set(block_start_index)

        elif set_type == TAIL_RID_TYPE:  # we have a tail to bring in,
            data = disk.read_tail_page_set(block_start_index)

        return data

    # kick out least recently used page from queue
    def __ensure_buffer_pool_can_fit_new_data(self, num_columns):
        # we must be careful here, due to the fact that dequeue will throw a max size exception. I may need to catch it somewhere
        while len(self.lru_enforcement) + num_columns + META_DATA_PAGES > MAX_PAGES_IN_BUFFER:
            table_name, page_range_index, page_set_index, set_type = self.lru_enforcement.popleft()

            # page is currently pinned and we cannot evict it
            if self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)] is not None:
                # page is currently in use and cannot be evicted
                # add it back to the queue
                self.lru_enforcement.append((table_name, page_range_index, page_set_index))

            # page can be evicted, remove
            else:
                self.__evict_page_set(table_name, page_range_index, page_set_index, set_type)

    # evaluate if page is dirty then remove any traces
    def __evict_page_set(self, table_name, page_range_index, page_set_index, set_type):
        if self.__is_dirty(table_name, page_range_index, page_set_index):
            meta = self.tables[table_name].__get_meta_data(page_range_index, page_set_index, set_type)
            self.__write_to_disk(page_range_index, page_set_index, table_name, set_type, meta)

        # remove entry from dictionary
        self.pages_mem_mapping.pop((table_name, page_range_index, page_set_index))

    # allocate new space for a page_set
    # assume meta data is packed
    def get_new_free_mem_space(self, table_name, page_range_index, page_set_index, num_columns, set_type):
        self.__ensure_buffer_pool_can_fit_new_data(num_columns)

        data = PageSet(num_columns + META_DATA_PAGES)
        # add data to the bufferpool and LRU queue
        self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)] = data, num_columns
        self.lru_enforcement.append((table_name, page_range_index, page_set_index))
        self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] = 1
        return data

        # also mark table_name, page_set_index as being in use right now

    def pin_page_set(self, table_name, page_range_index, page_set_index, set_type):
        # start at zero and build up
        if self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] is None:
            # add pair with 1 to indicate we just started pinning
            self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] = 1
        else:
            # increase the amount of users using the page, for M3 safe keeping
            self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] += 1

    def __is_dirty(self, table_name, page_range_index, page_set_index):
        return table_name, page_range_index, page_set_index in self.dirty_page_sets

    # called from table when the ref counter needs to be dec/removed
    def unpin_page_set(self, table_name, page_range_index, page_set_index, set_type):

        self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] -= 1

        # if the page is no longer in use, remove it from the mapping, in m3 we may decide to keep it
        if self.pinned_page_sets[(table_name, page_range_index, page_set_index, set_type)] == 0:
            self.pinned_page_sets.pop((table_name, page_range_index, page_set_index, set_type))

    # write dirty data to disk
    def flush_buffer_pool(self):
        for table in self.tables.values():
            table.disk.write_key_directory_set(table.keys, table.brid_block_start, table.trid_block_start)

        self.dirty_page_sets = sorted(self.dirty_page_sets)

        for (table_name, page_range_index, page_set, set_type) in self.dirty_page_sets:
            table = self.tables[table_name]
            meta_data = table.get_meta_data(page_range_index, page_set, set_type)
            self.__write_to_disk(table_name, page_range_index, page_set, set_type, meta_data)


    # allow a table to mark pagesets as dirty
    def mark_as_dirty(self, table_name, page_range_index, page_set_index, set_type):
        self.dirty_page_sets.add((table_name, page_range_index, page_set_index, set_type))


    @staticmethod
    def pack_data(page_set, rids, timestamps, schema, indirections, indirection_types):
        data = PageSet(len(page_set.pages) + META_DATA_PAGES)

        meta_start = len(page_set.pages)
        rids_data = []
        timestamps_data = []
        schema_data = []
        indirections_data = []
        indirection_types_data = []

        for i in range(len(rids)):
            for byte in int.to_bytes(rids[i], length=8, byteorder="little"):
                rids_data.append(byte)
            for byte in int.to_bytes(timestamps[i], length=8, byteorder="little"):
                timestamps_data.append(byte)
            for byte in int.to_bytes(schema[i], length=8, byteorder="little"):
                schema_data.append(byte)
            for byte in int.to_bytes(indirections[i], length=8, byteorder="little"):
                indirections_data.append(byte)
            for byte in int.to_bytes(indirection_types[i], length=8, byteorder="little"):
                indirection_types_data.append(byte)

        # append bytes to data properly
        bytes = bytearray(rids_data)
        if len(bytes) != PAGE_SIZE:
            invalids = bytearray(PAGE_SIZE - len(bytes))
            for i in range(len(invalids)):
                invalids[i] = -1
            bytes.extend(invalids)
        data.pages[meta_start].data = bytes

        bytes = bytearray(timestamps_data)
        if len(bytes) != PAGE_SIZE:
            invalids = bytearray(PAGE_SIZE - len(bytes))
            for i in range(len(invalids)):
                invalids[i] = -1
            bytes.extend(invalids)
        data.pages[meta_start + 1].data = bytes

        bytes = bytearray(schema_data)
        if len(bytes) != PAGE_SIZE:
            invalids = bytearray(PAGE_SIZE - len(bytes))
            for i in range(len(invalids)):
                invalids[i] = -1
            bytes.extend(invalids)
        data.pages[meta_start + 2].data = bytes

        bytes = bytearray(indirections_data)
        if len(bytes) != PAGE_SIZE:
            invalids = bytearray(PAGE_SIZE - len(bytes))
            for i in range(len(invalids)):
                invalids[i] = -1
            bytes.extend(invalids)
        data.pages[meta_start + 3].data = bytes

        bytes = bytearray(indirection_types_data)
        if len(bytes) != PAGE_SIZE:
            invalids = bytearray(PAGE_SIZE - len(bytes))
            for i in range(len(invalids)):
                invalids[i] = -1
            bytes.extend(invalids)
        data.pages[meta_start + 4].data = bytes

        return data


    @staticmethod
    def unpack_data(data):
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
        data, num_columns = self.pages_mem_mapping[(table_name, page_range_index, page_set_index, set_type)]
        self.__update_meta_data(data, meta)
        table = self.tables[table_name]
        disk = self.tables[table_name].disk
        if set_type is BASE_RID_TYPE:
            rid = [k for k, v in table.page_directory.items() if v[0] == page_range_index and v[1] == page_set_index]
            block_start_index = table.brid_block_start[rid[0]]  # just need a single rid that has the block start index
            disk.write_base_page_set(data, block_start_index)
        else:
            rid = [k for k, v in table.page_ranges[page_range_index].tail_rids.items() if v[0] == page_set_index]
            block_start_index = table.trid_block_start[rid[0]]  # just need a single rid that has the block start index
            disk.write_tail_page_set(data, block_start_index)

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
        indirections_ba = bytearray(indirections_data)
        pad_byte_array(indirections_ba)

        for num in indirection_types:
            if num: # since indirection type can be none, check if its none
                for byte in int.to_bytes(num, length=8, byteorder="little"):
                    indirection_types_data.append(byte)
        indirection_types_ba = bytearray(indirection_types_data)
        pad_byte_array(indirection_types_ba, 2)

        meta_start = len(data.pages) - META_DATA_PAGES
        data.pages[meta_start].data = rids_ba
        data.pages[meta_start + 1].data = timestamps_ba
        data.pages[meta_start + 2].data = schema_ba
        data.pages[meta_start + 3].data = indirections_ba
        data.pages[meta_start + 4].data = indirection_types_ba