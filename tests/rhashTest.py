from template.index import RHash, RHashNode
rhash = RHash()
"""rhash.insert(2, 2) # insert empty
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
print("--- Get Test ---")
for i in range(1_000_000):
    rhash.insert(i, i)"""

"""print('--- Sum ---')
print(rhash.get_sum(0,10))"""

print('--- Seed Testing ---')
print('inserting 1000')
for x in range(0,1001): 
    rhash.insert(x,x,False)  #seeds: head = 0, tail = 1000, mid = 500

rhash.check_and_build_seeds(True)
rhash.remove(500,500)
rhash.remove(400,400)
rhash.remove(1000,1000) #removing tail
rhash.remove(0,0)

print('\ninserting 2000') #inserting above 1000 entries
for x in range(0,2001): 
    rhash.insert(x,x,False)

rhash.check_and_build_seeds(True)
rhash.remove(500,500) #ISSUE: get 1999 and 2000 as seeds after removal (1999 is not helpful as a seed...)
rhash.check_and_build_seeds(True)



