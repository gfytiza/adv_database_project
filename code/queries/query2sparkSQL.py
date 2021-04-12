from pyspark.sql import SparkSession
import pandas as pd
import  sys
import time
spark = SparkSession.builder.appName("query2-sparkSQL").getOrCreate()


f =sys.argv[1]
if (f == 'csv') :

        ratings = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/exercise/ratings.csv")

elif ( f == 'parquet') :
        ratings = spark.read.format('parquet').options(header='false').load("hdfs://master:9000/exercise/ratings.parquet")

else:
        raise Exception ("Give a format!")



ratings.registerTempTable("ratings")

sqlString = \
        "select Rate from (" + \
        "select avg(_c2) as Rate " + \
        "from ratings " + \
        "group by _c0  " + \
        ") where Rate > 3.5 "

sql1 = \
        "select count( distinct _c0) " + \
        "from ratings "

start=time.time()

res = spark.sql(sqlString).toPandas()
res1 = spark.sql(sql1).toPandas()


print("The result is:")
print(100*res.shape[0]/res1.iat[0,0])

end=time.time()

print("Execution time of query is:")
print(end-start)
