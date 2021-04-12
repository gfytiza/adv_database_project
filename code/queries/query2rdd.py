from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("query2-rdd").getOrCreate()
import time
sc = spark.sparkContext

start=time.time()

res = \
        sc.textFile("hdfs://master:9000/exercise/ratings.csv"). \
        map(lambda x : (x.split(",")[0], (1,float(x.split(",")[2])))). \
        reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
        map(lambda x: (x[0], x[1][1]/x[1][0])). \
        map(lambda x: (1,(1,1)) if x[1]>3.5 else (1,(0,1))). \
        reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
        map(lambda x: 100*x[1][0]/x[1][1])



print("The result is:")
for i in res.collect():
        print(i)

end=time.time()

print("Time execution of query is:")
print(end-start)