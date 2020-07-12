Rdd = sc.parallelize([(1, 2), (3, 4), (3, 6), (4, 5)])

# Transform the rdd with countByKey()
total = Rdd.countByKey()

# What is the type of total?
print("The type of total is", type(total))

# Iterate over the total and print the output
for k, v in total.items(): 
  print("key", k, "has", v, "counts")
