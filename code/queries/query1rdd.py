from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

from io import StringIO
import csv
import time

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def profit(kostos, esoda):
        return ((int(esoda)-int(kostos))/int(kostos))*100

start=time.time()

res = \
        sc.textFile("hdfs://master:9000/exercise/movies.csv"). \
        map(lambda x: split_complex(x)).\
        filter(lambda x: x[3]!="" and int(x[3].split("-")[0])>=2000 and int(x[5])!=0 and int(x[6])!=0).\
        map(lambda x: (x[3].split("-")[0], (profit(x[5],x[6]),  x[1]))).\
        reduceByKey(lambda x, y: x if(x[0]>y[0]) else y).\
        sortByKey(ascending=True).\
        map(lambda x: (x[0], x[1][1], x[1][0]))



print("The result is:")
for i in res.collect():
    print(i)

end=time.time()

print(end-start)
