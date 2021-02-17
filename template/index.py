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

    def __init__(self, rid, value, prev_node = None, next_node = None):
        self.rid = rid
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
        self.dictionary = {} # column value to RHashNode
        self.startNode = None

    def insert(self, rid, value):
        #newNode = RHashNode(rid, value)
        # 3 cases:
        # no items in the dictionary
        if len(self.dictionary) == 0:
            __insert_into_empty_dictionary(rid, value)
        # None <--> newNode <--> None
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
        """
        while (nodeToCheck != None):
            if nodeToCheck.getValue() >= newNode.getValue():
                if (nodeToCheck.getPrevNode() != None):
                    prev_node = nodeToCheck.getPrevNode() #prev node?
                    if prev_node >= newNode.getValue():
        """
                        
        
        

    def __getClosestNode(self, value):
        # Implement later
        # following self.seeds, gets the node with closest value to "value"
        for node in self.dictionary:
            return node
        return None

    def __insert_into_empty_dictionary(self,rid,value): 
        new_node = RHashNode(rid,value)
        self.startNode = new_node
        self.dictionary[value] = self.startNode
        
        

    def __insert_into_single_item_dictionary(self, rid, value):
        # get the item in the dictionary
        extantNode = self.startNode
        newNode = RHashNode(rid, value)
        if extantNode.value > value:
            # need to have newNode be the head of the list
            extantNode.prev_node = newNode
            newNode.next_node = extantNode
            self.startNode = newNode
        else:
            # newNode needs to be the tail of the list
            extantNode.next_node = newNode
            newNode.prev_node = extantNode
        # update these nodes in self
        self.dictionary[value] = newNode
        self.dictionary[extantNode.value = extantNode]
        return


    def __insert_before_item_in_list(self,rid,value):
        
        current = self.startNode #should we start at seed instead of start node? (once we implement the seed)
        # Maybe just let this take in a node and assume that we are going to insert directly before the provided node
        # That way outside of this function we can just call __getClosestNode which will operate using seeds
        
        while current is not None:
            if current.getValue() >= value: # where to stop in ll- how do we want to decide 
                
                break
            else:
                current = current.next_node
        
        if current is None:
            raise NameError('Item is not in list')
            
        else:
            new_node = RHashNode(rid,value)
            new_node.prev_node = current.prev_node
            new_node.next_node = current
            if current.prev_node is not None:
                current.prev_node.next_node = new_node #updates previous node to point forwards to new node if a previous node exists
            current.prev_node = new_node #updates the current node to point backwards to the new node
            self.dictionary[value] = new_node
     
# node0 <--> node1 <--> node2
#        ^ node3

# node0 <--> node1 <--> node2
#    ^--node3--^

# node0 <--> node3 <--> node1 <--> node2