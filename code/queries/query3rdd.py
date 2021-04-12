from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("query3-rdd").getOrCreate()
import time
sc = spark.sparkContext

start=time.time()

ratings = \
        sc.textFile("hdfs://master:9000/exercise/ratings.csv"). \
        map(lambda x : (x.split(",")[1], (1,float(x.split(",")[2])))). \
        reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
        map(lambda x: (x[0], x[1][1]/x[1][0], x[1][0]))

genre = \
        sc.textFile("hdfs://master:9000/exercise/movie_genres.csv"). \
        map(lambda x : (x.split(",")[0], x.split(",")[1]))

res = ratings.join(genre). \
        map(lambda x : (x[1][1], (x[1][0], 1))). \
        reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1])). \
        map(lambda x: (x[0], x[1][0]/x[1][1], x[1][1])).\
        sortBy(lambda x: x[0], ascending=True)


print("The result is:")
for i in res.collect():
        print(i)

end=time.time()

print("Time execution of query is:")
print(end-start)
