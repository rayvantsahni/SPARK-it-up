// action 1: reduce()
sc.parallelize(1 to 10).reduce(_ + _)


// action 2: count()
sc.parallelize(List(1,5,6,5,7,5,1,5,1,5,1,5,1,8)).count()


// action 3: collect()
sc.parallelize(1 to 15).collect()


// action 4: take()
sc.parallelize(1 to 100).take(5)


// action 5: first()
sc.parallelize(1 to 15).first()


// action 6: max()
sc.parallelize(List(1,5,6,5,7,5,1,5,1,5,1,5,1,8)).max()


// action 7: min()
sc.parallelize(List(1,5,6,5,7,5,1,5,1,5,1,5,1,8)).min()
