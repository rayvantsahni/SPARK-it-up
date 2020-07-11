// transformation 1: distinct()
sc.parallelize(List(1,5,6,5,1,5,1,8)).distinct()


// transformation 2: filter()
sc.parallelize(1 to 10).filter(x => x%2 == 0).collect()


// transformation 3: map()
sc.parallelize(1 to 10).map(x => x * x)


// transformation 4: flatMap()
sc.parallelize("my name is apache spark").flatMap(x => List(x)).collect()


// transformation 5: sortBy
sc.parallelize(Seq("fiji", "greenland", "argentina", "egypt", "brazil", "australia", "russia")).sortBy(x => x.length()).collect()
