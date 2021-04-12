from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("query4-rdd").getOrCreate()

sc = spark.sparkContext

from io import StringIO
import csv

import time

def split_complex(x):
        return list(csv.reader(StringIO(x),delimiter=','))[0]


def decate (x) :
        if ( x == '' ) :
                return 100
        if ( int(x) >= 2000 and int(x)<= 2004) :
                return 1
        elif ( int(x) > 2004 and int(x) <= 2009 ) :
                return 2
        elif ( int(x)> 2009 and int(x) <= 2014 ) :
                return 3
        elif (int(x) > 2014 and int(x)<= 2019 ) :
                return 4
        else :
                return 100

def len_of_sum (x) :
        l = len(x.split())
        return l

start=time.time()

movies = \
        sc.textFile("hdfs://master:9000/exercise/movies.csv"). \
        map(lambda x : (split_complex(x)[0] , ( decate(split_complex(x)[3][:4]), len_of_sum(split_complex(x)[2]), split_complex(x)[1]) ) )

genre = \
        sc.textFile("hdfs://master:9000/exercise/movie_genres.csv"). \
        map(lambda x : (x.split(",")[0], x.split(",")[1]))

res = movies.join(genre). \
        map(lambda x : (x[1][0][0], (x[1][0][1], 1, x[1][1])) if x[1][1] == 'Drama' else (100, (100, 100, 100)) ). \
        reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1])). \
        map(lambda x: (x[0], x[1][0]/x[1][1], x[1][1])).\
        sortBy(lambda x: x[0], ascending=True)



print("The result is:")
for i in res.collect():
        if (i[0] != 100) :
                print(i)

end=time.time()

print("Time execution of query is:")
print(end-start)
