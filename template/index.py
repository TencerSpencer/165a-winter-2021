from template.config import *
"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
In-Memory ONLY
"""
class Index:

    def __init__(self, table):
        table.set_index(self)
        self.table = table
        self.indices = [None] *  table.num_columns 
        for i in range(table.num_columns):
            self.indices[i] = RHash()

    """
    # returns the location (RID?) of all records with the given value on column "column"
    """

    def locate(self, column, value): #how do we identify which index is for which column?
        indexRhash = self.indices[column]
        # indexRhash[value].rids 
        return indexRhash.get(value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, column, begin, end):
        indexRhash = self.indices[column]
        return indexRhash.get_range(begin, end)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        query_cols = [None] * self.table.num_columns
        query_cols[column_number] = 1
        # [None, None, 1, None, None]
        for key in self.table.keys:
            record = self.table.select_record(key, query_cols)
            self.indices[column_number].insert(record[1][column_number], record[0], False)
        self.indices[column_number].check_and_build_seeds()
    
    """ Checks if the index is built for column "column" """
    def is_index_built(self, column_number):
        if column_number >= self.table.num_columns:
            return False
        return self.indices[column_number].getSize != 0

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        self.indices[column_number] = RHash()

    """Sum over the specified range"""
    def get_sum(self, column, begin, end):
        return self.get_sum(column, begin, end)

    """ Update the rid of the provided value in the provided column"""
    def update_index(self, column_number, value, old_rid, new_rid):
        if column_number >= self.table.num_columns:
            return
        self.indices[column_number].remove(value, old_rid)
        self.indices[column_number].insert(value, new_rid)

    def insert_into_index(self, column_number, value, rid):
        if column_number >= self.table.num_columns:
            return
        self.indices[column_number].insert(value, rid, true)



class RHashNode:
    def __init__(self, value, rid, prev_node = None, next_node = None):
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
        self.dictionary = {} # column value to RHashNode
        self.head = None
        self.tail = None
        self.size = 0 # Keeps track of the number of VALUES, NOT RIDs
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
        while node != None and node.value <= end:
            rids.extend(node.get_RIDs())
            node = node.next_node
        return rids

    def get_sum(self, begin, end):
        sum = 0
        node = self.__getClosestNode(begin)
        while node != None and node.value <= end:
            sum += node.value * len(node.get_RIDs())
            node = node.next_node
        return sum

    """ Rebuilds the seeds if size changes by some amount """
    def check_and_build_seeds(self):
        if self.size >= self.last_seed_build_size * SEED_REBUILD_THRESH or self.last_seed_build_size > self.size * SEED_REBUILD_THRESH:
            # Rebuild seeds
            # We want 3 seeds for 100 <= size < 1000
            num_seeds = 1 # Not including the head, tail
            if self.size >= 1000:
                num_seeds = int(self.size / 1_000)
            # Init seeds
            self.seeds = []
            node = self.head
            interval = int(self.size * 1/(num_seeds+1))
            counter = 0
            while node.next_node != None:
                if (counter % interval) == 0:
                    self.seeds.append(node)
                counter += 1
                node = node.get_next_node()
            # add the tail
            self.seeds.append(self.tail)


    def __insert_into_empty_dictionary(self, value, rid): 
        new_node = RHashNode(value, rid)
        self.head = new_node
        self.tail = new_node
        self.tail.next_node = None
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
            self.check_and_build_seeds()
        return

    def remove(self, value, rid):
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
            del self.dictionary[value]
            self.check_and_build_seeds()
        else:
            # just remove the rid from value array
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
        # Determine whether we need to go up or down
        if closestNode.prev_node != None:
            while closestNode.prev_node.value > value:
                # Traverse DOWN
                closestNode = closestNode.prev_node
        while closestNode.value < value:
            # Traverse UP
            closestNode = closestNode.next_node
        return closestNode



    def __insert_before_item_in_list(self, nextNode, value, rid):
        new_node = RHashNode(value, rid)
        
        #should we start at seed instead of start node? (once we implement the seed)
        # Maybe just let this take in a node and assume that we are going to insert directly before the provided node
        # That way outside of this function we can just call __getClosestNode which will operate using seeds
        new_node.prev_node = nextNode.prev_node
        new_node.next_node = nextNode
        if nextNode.prev_node is not None:
            nextNode.prev_node.next_node = new_node #updates previous node to point forwards to new node if a previous node exists
        nextNode.prev_node = new_node #updates the current node to point backwards to the new node
        self.dictionary[value] = new_node
        


    def __append(self, value, rid):
        new_node = RHashNode(value,rid)
        new_node.prev_node = self.tail
        self.tail.next_node = new_node
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
        