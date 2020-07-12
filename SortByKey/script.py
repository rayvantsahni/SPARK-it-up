Rdd = sc.parallelize([(1,2),(3,4),(3,6),(4,5)])

Rdd_Reduced = Rdd.reduceByKey(lambda x, y: x+y)

# Sort the reduced RDD with the key by descending order
Rdd_Reduced_Sort = Rdd_Reduced.sortByKey(ascending=False)
# Iterate over the result and print the output
for num in Rdd_Reduced_Sort.collect():
  print("Key {} has {} Counts".format(num[0], num[1]))
