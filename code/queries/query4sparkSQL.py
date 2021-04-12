from pyspark.sql import SparkSession
import pandas as pd
import sys
import time

spark = SparkSession.builder.appName("query4-sparkSQL").getOrCreate()

def decate (x) :
        if ( x >= 2000 and x <= 2004 ):
                return 1
        elif ( x>2004 and x<=2009 ):
                return 2
        elif ( x > 2009 and x<=2014 ):
                return 3
        elif ( x > 2014 and x<=2019 ):
                return 4
def len_of_sum (x) :
        x1 = str(x)
        if (not x) :
                return 0
        l = len(x1.split())
        return l

f =sys.argv[1]

if (f == 'csv') :

        movies = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/exercise/movies.csv")
        genre = spark.read.format('csv').options(header='false', inferSchema='true').load("hdfs://master:9000/exercise/movie_genres.csv")
elif ( f == 'parquet') :
        movies = spark.read.format('parquet').options(header='false').load("hdfs://master:9000/exercise/movies.parquet")

        genre = spark.read.format('parquet').options(header='false').load("hdfs://master:9000/exercise/movie_genres.parquet")

else:
        raise Exception ("Give a format!")


movies.registerTempTable("movies")
genre.registerTempTable("genre")
spark.udf.register("formatter", decate)
spark.udf.register("formatter1", len_of_sum)

sqlString = \
        "select avg(t1.var2), t1.var1, count(t1.var1) " + \
        "from (select formatter(YEAR(_c3)) as var1, _c0, formatter1(_c2) as var2 " + \
        "from movies " + \
        "where YEAR(_c3) >= 2000 ) as t1, genre as g " + \
        "where t1._c0 == g._c0 and g._c1 == 'Drama' " + \
        "group by t1.var1 " + \
        "order by t1.var1 "

start=time.time()

res = spark.sql(sqlString).toPandas()


print("The result is:")
print(res)

end=time.time()
