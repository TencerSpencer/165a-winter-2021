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
print("--- Get Test ---")
for i in range(1000):
    rhash.insert(i, i)
print("75 - 80")
print(rhash.get_range(75, 80))
print("0 - 10") 
print(rhash.get_range(0, 10 ))
print("-10 - 15")
print(rhash.get_range(-10, 15 ))
print("998 - 1500")
print(rhash.get_range(998, 1500))

print('--- Sum ---')
print(rhash.get_sum(0,10))


