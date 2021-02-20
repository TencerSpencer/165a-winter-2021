"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
In-Memory ONLY
"""

class Index:

    def __init__(self, table):
        # One index for each table (?) All are empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location (RID?) of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, column, begin, end):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass


class RHashNode:
    def __init__(self, value, rid, prev_node = None, next_node = None):
        self.rids = []
        self.rids.append(rid)
        self.value = value
        self.prev_node = prev_node
        self.next_node = next_node

    def getValue(self):
        return value

    def getRID(self):
        return rid

    def getPrevNode(self):
        return prev_value

    def getNextNode(self):
        return next_value

class RHash:

    def __init__(self):
        # self.smallest_value
        # self.largest_value
        # self.seeds = [smallest, midpoint, largest]
        self.seeds = []
        self.dictionary = {} # column value to RHashNode
        self.head = None
        self.tail = None

    def __insert_into_empty_dictionary(self, value, rid): 
        new_node = RHashNode(value, rid)
        self.head = new_node
        self.tail = new_node
        self.tail.next_node = None
        self.dictionary[value] = new_node

    def insert(self, value, rid):
            # Case 1: Item exists in the dictionary
        if value in self.dictionary:
            self.dictionary[value].rids.append(rid)
            return
        # Case 2: Dictionary is empty
        if len(self.dictionary) == 0:
            self.__insert_into_empty_dictionary(value, rid)
            return
        # Case 3: Value is less than the head; push
        if value < self.head.value:
            self.__push(value, rid)
            return
        # Case 4: Value is greater than the tail; append
        if value > self.tail.value:
            self.__append(value, rid)
            return
        # Case 5: Somewhere in the middle
        closestNode = self.__getClosestNode(value)
        # print(closestNode.value)(value)
        # traverse the doubly-linked list to find a node such that:
        # closestNode.value 
        # closestNode.value > value AND closestNode.prev_node.value < value
        # Determine whether we need to go up or down
        if closestNode.prev_node != None:
            while closestNode.prev_node.value > value:
                # Traverse DOWN
                closestNode = closestNode.prev_node
        while closestNode.value < value:
            # Traverse UP
            closestNode = closestNode.next_node
        # Found closest node to meet conditions
        self.__insert_before_item_in_list(closestNode, value, rid)
        return
    
    def remove(self, value, rid):
        if len(self.dictionary[value].rids) == 1:

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
        else:
            # just remove the rid from value array
            self.dictionary[value].rids.remove(rid)

    def __getClosestNode(self, value):
        if len(self.seeds) == 0:
            return self.head
        closestNode = None
        maxDelta = float(inf)
        for node in self.seeds:
            newDelta = math.abs(value - node.value)
            if newDelta < maxDelta:
                closestNode = node
                maxDelta = newDelta
        return closestNode


    """
    def __insert_into_single_item_dictionary(self, rid, value):
        # get the item in the dictionary
        extantNode = self.head
        newNode = RHashNode(rid, value)
        if extantNode.value > value:
            # need to have newNode be the head of the list
            extantNode.prev_node = newNode
            newNode.next_node = extantNode
            self.head = newNode
        else:
            # newNode needs to be the tail of the list
            extantNode.next_node = newNode
            newNode.prev_node = extantNode
            self.tail = newNode
        # update these nodes in self
        self.dictionary[value] = newNode
        self.dictionary[extantNode.value] = extantNode
        return

    """
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
        
     
# node0 <--> node1 <--> node2
#        ^ node3

# node0 <--> node1 <--> node2
#    ^--node3--^

# node0 <--> node3 <--> node1 <--> node2

"""
    def insert(self, rid, value):
        #newNode = RHashNode(rid, value)
        # 4 cases:
        # Case 1: Item exists in the dictionary
        if rid in self.dictionary:
            self.dictionary[value].value.append(rid)
            return
        # no items in the dictionary
        # None <--> newNode <--> None
        if len(self.dictionary) == 0:
            __insert_into_empty_dictionary(rid, value)
        # 1 item in the dictionary
        # None <--> newNode <--> nodeToCheck <--> None
        # OR None <--> nodeToCheck <--> newNode <--> None
        if len(self.dictionary) == 1:
            __insert_into_single_item_dictionary(rid, value)
        # 2+ items in the dictionary
        
        # find the prev node and next node
        # trying to find the node with largest value less than "value"
        # and the node with the smallest value greater than "value"
        closestNode = __getClosestNode(value)
        # traverse the doubly-linked list to find a node such that:
        # node.value <= newNode.value AND
        # node.next_node.value >= newNode.value
        node = None
        if closestNode.value <= value:
            node = traverseUp(closestNode, value)
        else:
            node = traverseDown(closestNode, value)
        # Now, (node.value <= value OR node == None) AND (node.next_node.value >= value OR node == None)
        # Can we just insert before?
        
        # Oh my god why are there so many cases
        while (nodeToCheck != None):
            if nodeToCheck.getValue() >= newNode.getValue():
                if (nodeToCheck.getPrevNode() != None):
                    prev_node = nodeToCheck.getPrevNode() #prev node?
                    if prev_node >= newNode.getValue():
        """