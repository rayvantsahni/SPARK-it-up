# WORD COUNT PROBLEM

# importing data
data = sc.textFile("/FileStore/tables/ai_ml_security_book.txt") 
data.take(15)


# splitting and changing the records to lower case
data_split = data.flatMap(lambda a: a.lower().split(" "))
data_split.take(10)
data_split.count()


# converting the split records into key-value pair
data_split_with_values = data_split.map(lambda w: (w, 1))
data_split_with_values.take(50)


# adding all the values for a key
result_rdd = data_split_with_values.reduceByKey(lambda a,b: a+b)
result_rdd.take(20)


# swapping the key-value for each record
result_rdd_swap = result_rdd.map(lambda a: (a[1], a[0]))
result_rdd_swap.take(10)


# sorting into acsending order of occurences
result_rdd_swap_sort_asc = result_rdd_swap.sortByKey()
result_rdd_swap_sort_asc.take(15)

# sorting into descending order of occurences
result_rdd_swap_sort_des = result_rdd_swap.sortByKey(ascending = False)
result_rdd_swap_sort_des.take(15)
