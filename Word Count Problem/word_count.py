# WORD COUNT PROBLEM in PYSPARK

# importing data
data = sc.textFile("/FileStore/tables/ai_ml_security_book.txt") 

# splitting and changing the records to lower case
data_split = data.flatMap(lambda a: a.lower().split(" "))
data_split.take(10)
data_split.count()

# converting the split records into key-value pair
data_split_with_values = data_split.map(lambda w: (w, 1))

# adding all the values for a key
result_rdd = data_split_with_values.reduceByKey(lambda a,b: a+b)

# swapping the key-value for each record
result_rdd_swap = result_rdd.map(lambda a: (a[1], a[0]))

# sorting into acsending order of occurences
result_rdd_swap_sort_asc = result_rdd_swap.sortByKey()

# sorting into descending order of occurences
result_rdd_swap_sort_des = result_rdd_swap.sortByKey(ascending = False)
result_rdd_swap_sort_des.take(15)
