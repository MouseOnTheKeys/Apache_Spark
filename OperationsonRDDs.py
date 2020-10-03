# RDD Persistence and Re-use
# Using a RDD for Multiple Actions Without Persistence
originalrdd = sc.parallelize([0, 1, 2, 3, 4, 5, 6, 7, 8])
newrdd = originalrdd.filter(lambda x: x % 2)
noelements = newrdd.count()
# processes newrdd
listofelements = newrdd.collect()
# reprocesses newrdd
print("There are {} elements in the collection {}").format(noelements, listofelements)
# returns:
# There are 4 elements in the collection [1, 3, 5, 7]

#! Using a RDD for Multiple Actions with Persistence
originalrdd = sc.parallelize([0, 1, 2, 3, 4, 5, 6, 7, 8])
newrdd = originalrdd.filter(lambda x: x % 2)
newrdd.persist()
"""After a request is made to persist the RDD using the persist() method
#(there is also a similar cache() method as well), the RDD will be kept in
memory on all of the nodes in the cluster where it is computed after the first
action called on it."""
noelements = newrdd.count()
# processes and persists newrdd in memory
listofelements = newrdd.collect()
# does not have to recompute newrdd
print("There are {} elements in the collection {}".format(noelements,listofelements)
# returns:
# There are 4 elements in the collection [1, 3, 5, 7]