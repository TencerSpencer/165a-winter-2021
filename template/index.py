from template.config import *
import threading

"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
In-Memory ONLY
"""


class Index:

    def __init__(self, table):
        table.set_index(self)
        self.table = table
        self.indices = [None] * table.num_columns
        self.isBuilt = [False] * table.num_columns
        self.rw_locks = [ReadWriteLock()] * table.num_columns
        for i in range(table.num_columns):
            self.indices[i] = RHash()

    """
    # returns the location (RID?) of all records with the given value on column "column"
    """

    def locate(self, column, value):
        self.rw_locks[column].acquire_read()
        output = self.indices[column].get(value)
        self.rw_locks[column].release_read()
        return output

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, column, begin, end):
        self.rw_locks[column].acquire_read()
        output = self.indices[column].get_range(begin, end)
        self.rw_locks[column].release_read()
        return output

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.rw_locks[column_number].acquire_write()
        query_cols = [None] * self.table.num_columns
        query_cols[column_number] = 1
        # [None, None, 1, None, None]
        keys = self.table.safe_get_keys()
        for key in keys:
            record = self.table.select_record(key, query_cols)
            self.indices[column_number].insert(record[1][column_number], record[0], False)
        self.indices[column_number].check_and_build_seeds(False)
        self.isBuilt[column_number] = True
        self.rw_locks[column_number].release_write()

    """ Checks if the index is built for column 'column' """

    def is_index_built(self, column_number):
        if column_number >= self.table.num_columns:
            return False
        self.rw_locks[column_number].acquire_read()
        output = self.isBuilt[column_number]
        self.rw_locks[column_number].release_read()
        return output

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.rw_locks[column_number].acquire_read()
        self.indices[column_number] = RHash()
        self.isBuilt[column_number] = False
        self.rw_locks[column_number].release_read()

    """Sum over the specified range"""

    def get_sum(self, column, begin, end):
        self.rw_locks[column].acquire_read()
        output = self.indices[column].get_sum(begin, end)
        self.rw_locks[column].release_read()
        return output

    """ Update the rid of the provided value in the provided column"""

    def update_rid(self, column_number, value, old_rid, new_rid):
        if column_number >= self.table.num_columns:
            return
        self.rw_locks[column_number].acquire_write()

        self.indices[column_number].remove(value, old_rid)
        self.indices[column_number].insert(value, new_rid)

        self.rw_locks[column_number].release_write()

    """ Update the value of the provided column and rid """

    def update_value(self, column_number, old_value, new_value, rid):
        if column_number >= self.table.num_columns:
            return
        self.rw_locks[column_number].acquire_write()

        self.indices[column_number].remove(old_value, rid)
        self.indices[column_number].insert(new_value, rid, True)

        self.rw_locks[column_number].release_write()

    def insert_into_index(self, column_number, value, rid):
        if column_number >= self.table.num_columns:
            return
        self.rw_locks[column_number].acquire_write()
        self.indices[column_number].insert(value, rid, True)
        self.rw_locks[column_number].release_write()

    def delete(self, column_number, value, rid):
        if column_number >= self.table.num_columns:
            return
        self.rw_locks[column_number].acquire_write()
        self.indices[column_number].remove(value, rid)
        self.rw_locks[column_number].release_write()


class RHashNode:
    def __init__(self, value, rid, prev_node=None, next_node=None):
        self.rids = []
        self.rids.append(rid)
        self.value = value
        self.prev_node = prev_node
        self.next_node = next_node

    def get_value(self):
        return self.value

    def get_RIDs(self):
        return self.rids

    def get_prev_node(self):
        return self.prev_node

    def get_next_node(self):
        return self.next_node


class RHash:

    def __init__(self):
        self.seeds = []
        self.dictionary = {}  # column value to RHashNode
        self.head = None
        self.tail = None
        self.size = 0  # Keeps track of the number of VALUES, NOT RIDs
        self.last_seed_build_size = 0

    def get(self, value):
        # Error checking?
        if self.dictionary.get(value, None) == None:
            return []
        return self.dictionary[value].rids

    def get_size(self):
        return self.size

    def get_range(self, begin, end):
        # get the first node
        rids = []
        node = self.__getClosestNode(begin)
        if node.value < begin:
            return []
        while node != None and node.value <= end:
            rids.extend(node.get_RIDs())
            node = node.next_node
        return rids

    def get_sum(self, begin, end):
        sum = 0
        node = self.__getClosestNode(begin)
        # print(node == self.tail)
        while node != None and node.value <= end:
            sum += node.value * len(node.get_RIDs())
            node = node.next_node
        return sum

    """ Rebuilds the seeds if size changes by some amount """

    def check_and_build_seeds(self, override):
        # print("Current size: " + str(self.size) + " Last build size: " + str(self.last_seed_build_size))
        if override or self.size >= self.last_seed_build_size * SEED_REBUILD_THRESH or self.last_seed_build_size > self.size * SEED_REBUILD_THRESH:
            # Rebuild seeds
            self.last_seed_build_size = self.size
            # We want 3 seeds for 100 <= size < 1000
            num_seeds = 1  # Not including the head, tail
            if self.size >= 1000:
                num_seeds = int(self.size / 1_000)
                # Init seeds
            self.seeds = []
            node = self.head
            if node == None:
                return
            interval = int(self.size * 1 / (num_seeds + 1)) + 1
            # print(interval)
            counter = 0
            while node.next_node != None:
                if (counter % interval) == 0:
                    self.seeds.append(node)
                counter += 1
                node = node.get_next_node()
            # add the tail
            self.seeds.append(self.tail)
            # for seed in self.seeds:
            #    print(seed.value)

    def __insert_into_empty_dictionary(self, value, rid):
        new_node = RHashNode(value, rid)
        self.head = new_node
        self.tail = new_node
        self.tail.next_node = None
        self.head.prev_node = None
        self.dictionary[value] = new_node

    def insert(self, value, rid, checkSeeds):
        # Case 1: Item exists in the dictionary
        if value in self.dictionary:
            self.dictionary[value].rids.append(rid)
            return
        # Since we know we will be adding a new value, increment the size
        self.size += 1
        # Case 2: Dictionary is empty
        if len(self.dictionary) == 0:
            self.__insert_into_empty_dictionary(value, rid)
        # Case 3: Value is less than the head; push
        elif value < self.head.value:
            self.__push(value, rid)
        # Case 4: Value is greater than the tail; append
        elif value > self.tail.value:
            self.__append(value, rid)
        else:
            # Case 5: Somewhere in the middle
            closestNode = self.__getClosestNode(value)
            # print(closestNode.value)(value)
            # traverse the doubly-linked list to find a node such that:
            # closestNode.value > value AND closestNode.prev_node.value < value

            # Found closest node to meet conditions
            self.__insert_before_item_in_list(closestNode, value, rid)
            # print("Inserted " + str(value) + " into list")
        if checkSeeds:
            self.check_and_build_seeds(False)
        return

    def remove(self, value, rid):
        # if self.dictionary.get(value, None) == None:
        #    return
        # print(self.dictionary)

        if len(self.dictionary[value].rids) == 1:
            self.size -= 1
            # Completely remove this entry
            nodeToRemove = self.dictionary[value]
            if nodeToRemove == self.head:
                self.head = self.head.next_node
                self.head.prev_node = None
            elif nodeToRemove == self.tail:
                self.tail = self.tail.prev_node
                self.tail.next_node = None
            else:
                nodeToRemove.prev_node.next_node = nodeToRemove.next_node
                nodeToRemove.next_node.prev_node = nodeToRemove.prev_node
            # self.dictionary.pop(value)
            if self.dictionary[value] in self.seeds:
                self.seeds.remove(self.dictionary[value])
                self.check_and_build_seeds(True)
            del self.dictionary[value]
            self.check_and_build_seeds(False)
        elif rid in self.dictionary[value].rids:
            # just remove the rid from value array
            # print(self.dictionary[value].rids)
            self.dictionary[value].rids.remove(rid)

    def __getClosestNode(self, value):
        closestNode = None
        if len(self.seeds) == 0:
            closestNode = self.head
        else:
            maxDelta = float('inf')
            for node in self.seeds:
                newDelta = abs(value - node.value)
                if newDelta < maxDelta:
                    closestNode = node
                    maxDelta = newDelta
                else:
                    break
        # Determine whether we need to go up or down
        if closestNode.prev_node != None:
            while closestNode.prev_node.value > value:
                # Traverse DOWN
                closestNode = closestNode.prev_node

        while closestNode.value < value and closestNode.next_node != None:
            # Traverse UP
            closestNode = closestNode.next_node
        return closestNode

    def __insert_before_item_in_list(self, nextNode, value, rid):
        new_node = RHashNode(value, rid)

        # should we start at seed instead of start node? (once we implement the seed)
        # Maybe just let this take in a node and assume that we are going to insert directly before the provided node
        # That way outside of this function we can just call __getClosestNode which will operate using seeds
        new_node.prev_node = nextNode.prev_node
        new_node.next_node = nextNode
        if nextNode.prev_node is not None:
            nextNode.prev_node.next_node = new_node  # updates previous node to point forwards to new node if a previous node exists
        nextNode.prev_node = new_node  # updates the current node to point backwards to the new node
        self.dictionary[value] = new_node

    def __append(self, value, rid):
        new_node = RHashNode(value, rid)
        self.tail.next_node = new_node
        new_node.prev_node = self.tail
        self.tail = new_node
        # print(self.tail.prev_node)
        self.dictionary[value] = new_node

    def __push(self, value, rid):
        new_node = RHashNode(value, rid)
        self.head.prev_node = new_node
        new_node.next_node = self.head
        self.head = new_node
        self.dictionary[value] = new_node

    def printList(self):
        node = self.head
        while (node != None):
            print(node.rids)
            node = node.next_node


class ReadWriteLock:
    def __init__(self):
        # Conditions allow our release_read to occur even after the lock has
        # been acquired with acquire_write
        self.lock = threading.Condition(threading.Lock())
        self.num_readers = 0

    def acquire_read(self):
        self.lock.acquire()
        try:
            self.num_readers += 1
        finally:
            self.lock.release()

    def release_read(self):
        self.lock.acquire()
        try:
            self.num_readers -= 1
            if not self.num_readers:
                self.lock.notifyAll()
        finally:
            self.lock.release()

    def acquire_write(self):
        self.lock.acquire()
        while self.num_readers > 0:
            self.lock.wait()

    def release_write(self):
        self.lock.release()