from pyspark.sql import SparkSession
import pandas as pd
import sys
import time

spark = SparkSession.builder.appName("query1-sparkSQL").getOrCreate()



f =sys.argv[1]
if (f == 'csv') :

        movies = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/exercise/movies.csv")

elif ( f == 'parquet') :
        movies = spark.read.format('parquet').options(header='false').load("hdfs://master:9000/exercise/movies.parquet")



else:
        raise Exception ("Give a format!")


movies.registerTempTable("movies")

sqlString = \
        "select tl.Year, m._c1 as Movie, tl.Max_profit "+\
        "from (select YEAR(_c3) as Year,  MAX((100*(_c6-_c5)/_c5)) as Max_profit " +\
        "from movies " +\
        "where YEAR(_c3) != 0 and YEAR(_c3) >= 2000 and _c5 != 0 and _c6 != 0 "+\
        "group by Year ) as tl, movies as m "+\
        "where tl.Year == YEAR(m._c3) and tl.Max_profit == (100*(m._c6-m._c5)/m._c5) "+\
        "order by tl.Year "


start=time.time()

res = spark.sql(sqlString).toPandas()


print("The result is:")
print(res)

end=time.time()

print("Execution time of query is:")
print(end-start)
