from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("repartition-join").getOrCreate()
import time

sc = spark.sparkContext

from io import StringIO
import csv


def minor(lst, key):
    lst3 = [value for value in lst if value != key]
    return tuple(lst3)


def funtel(seq):
    l=[]
    r=[]
    for (n, v) in seq:
        if(n==1):
            l.append(v)
        else:
            r.append(v)
    return [(v, w) for v in l for w in r]




def repartition_join(path_data1, path_data2, num_of_columns_data1, num_of_columns_data2, key_index_data1, key_index_data2):
    data1 = \
            sc.textFile(path_data1).\
            map(lambda x: x.split(",")).\
            map(lambda x: (x[key_index_data1],(1, (x[key_index_data1], (minor(x[:num_of_columns_data1], x[key_index_data1]))))))


    data2 = \
            sc.textFile(path_data2).\
            map(lambda x: x.split(",")).\
            map(lambda x: (x[key_index_data2],(2, (x[key_index_data2], (minor(x[:num_of_columns_data2], x[key_index_data2]))))))




    t1 = time.time()

    d = \
            data1.union(data2).\
            groupByKey().\
            flatMap(lambda x: funtel(x[1])).\
            map(lambda x: (tuple(x[0]), tuple(x[1]))).\
            map(lambda x: (x[0][0], tuple(x[0][1]), tuple(x[1][1])))

    t2 = time.time()

#    for i in d.take(20):
#        print(i)

    print("time is:")
    print(t2-t1)

path1 = "hdfs://master:9000/exercise/movie_genres_100.csv"
path2 = "hdfs://master:9000/exercise/ratings.csv"

repartition_join(path1, path2, 2, 4, 0, 1)

#num_of_columns_data1 = 2
#num_of_columns_data2 = 4
#path_data1 = "hdfs://master:9000/exercise/movie_genres_100.csv"
#path_data2 = "hdfs://master:9000/exercise/ratings.csv"
#key_index_data1 = 0
#key_index_data2 = 1
