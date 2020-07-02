# Databricks notebook source
data = sc.textFile("/FileStore/tables/ai_ml_security_book.txt")

# COMMAND ----------

data.take(15)

# COMMAND ----------

data_split = data.flatMap(lambda a: a.lower().split(" "))

# COMMAND ----------

data_split.take(10)

# COMMAND ----------

data_split.count()

# COMMAND ----------

data_split_with_values = data_split.map(lambda w: (w, 1))

# COMMAND ----------

data_split_with_values.take(50)

# COMMAND ----------

result_rdd = data_split_with_values.reduceByKey(lambda a,b: a+b)

# COMMAND ----------

result_rdd.take(20)

# COMMAND ----------

result_rdd_swap = result_rdd.map(lambda a: (a[1], a[0]))

# COMMAND ----------

result_rdd_swap.take(10)

# COMMAND ----------

result_rdd_swap_sort_asc = result_rdd_swap.sortByKey()

# COMMAND ----------

result_rdd_swap_sort_asc.take(15)

# COMMAND ----------

result_rdd_swap_sort_des = result_rdd_swap.sortByKey(ascending = False)

# COMMAND ----------

result_rdd_swap_sort_des.take(15)

# COMMAND ----------


