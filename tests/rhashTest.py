from template.index import RHash, RHashNode

rhash = RHash()
rhash.insert(2, 2) # insert empty
rhash.insert(4, 4) #insert into tail
rhash.insert(1,1) #insert as head
rhash.insert(3,3) # insert in the middle
rhash.insert(3,5) # insert duplicate value
rhash.printList()
print("---")
rhash.remove(3,3) # Remove duplicate
rhash.printList()
print("---")
rhash.remove(3,5) # Remove whole entry
rhash.printList()
print("---")
rhash.remove(1,1) # Remove head
rhash.printList()
print("---")
rhash.remove(4,4) # Remove tail
rhash.printList()

