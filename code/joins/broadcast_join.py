import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("broadcastjoin-rdd").getOrCreate()

sc = spark.sparkContext

def broadcast_join ( x, y):
    rdd_a = sc.textFile("hdfs://master:9000/exercise/"+x).map(lambda x : (x.split(",")[1], (x.split(',')[0], x.split(',')[2], x.split(',')[3])))
#    print(rdd_a.take(3))

    bmap = sc.textFile("hdfs://master:9000/exercise/"+y).map(lambda x : (x.split(",")[0], (tuple(x.split(",")[1:])))).collectAsMap()
#    b=sc.textFile("hdfs://master:9000/exercise/"+y).map(lambda x : (x.split(",")[0], (tuple(x.split(",")[1]))))
#    print(b.take(3))
    t1=time.time()
    bmap_broad = sc.broadcast(bmap)

    join_a_b = rdd_a.map(lambda x: (x[0], bmap_broad.value.get(x[0], 'None'), x[1])).map(lambda x: x if x[1] != 'None' else None)
    print('time is :')
    print( time.time()-t1)

#    for i in join_a_b.collect():
#        if ( i != None ):
#            print(i)

#    t=time.time()
#    res =rdd_a.join(b)
#    print('time is :')
#    print(time.time()-t)

broadcast_join('ratings.csv','movie_genres_100.csv')
