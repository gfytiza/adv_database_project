from pyspark.sql import SparkSession
import pandas as pd
import sys
import time
spark = SparkSession.builder.appName("query333-sparkSQL").getOrCreate()


f =sys.argv[1]
if (f == 'csv') :

        ratings = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/exercise/ratings.csv")
        genre = spark.read.format('csv').options(header='false', inferSchema='true').load("hdfs://master:9000/exercise/movie_genres.csv")
elif ( f == 'parquet') :
        ratings = spark.read.format('parquet').options(header='false').load("hdfs://master:9000/exercise/ratings.parquet")

        genre = spark.read.format('parquet').options(header='false').load("hdfs://master:9000/exercise/movie_genres.parquet")

else:
        raise Exception ("Give a format!")


ratings.registerTempTable("ratings")
genre.registerTempTable("genre")

sqlString = \
        "select g._c1, avg(t.mean), count(g._c0) " + \
        "from (select avg(_c2) as mean, _c1 from ratings group by _c1) as t, genre as g " + \
        "where t._c1 == g._c0 " + \
        "group by g._c1 " + \
        "order by g._c1 "

start=time.time()

res = spark.sql(sqlString).toPandas()


print("The result is:")
print(res)
end=time.time()

print("Time execution of query is:")
print(end-start)

