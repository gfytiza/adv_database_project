from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("query5-rdd").getOrCreate()

sc = spark.sparkContext

from io import StringIO
import csv
import time

def split_complex(x):
        return list(csv.reader(StringIO(x),delimiter=','))[0]


start=time.time()

ratings = \
        sc.textFile("hdfs://master:9000/exercise/ratings.csv"). \
        map(lambda x : (x.split(",")[1], (x.split(",")[0],float(x.split(",")[2]))))


genre = \
        sc.textFile("hdfs://master:9000/exercise/movie_genres.csv"). \
        map(lambda x : (x.split(",")[0], x.split(",")[1]))

movies = \
        sc.textFile("hdfs://master:9000/exercise/movies.csv"). \
        map(lambda x : (split_complex(x)[0] , (split_complex(x)[1], float(split_complex(x)[7])) ) )

######neo

res1 = genre.join(ratings). \
        map(lambda x : ((x[1][0], x[1][1][0]), 1)). \
        reduceByKey(lambda x, y : x+y ). \
        map(lambda x: (x[0][0],( x[0][1], x[1]) )).\
        reduceByKey(lambda x,y: (x[0],x[1]) if (x[1]>y[1]) else (y[0],y[1]) ).\
        map(lambda x: (x[1][0],(x[0],x[1][1])))
##des jana gia history
# xrhsth, eidos , plhuos

res2 = movies.join(ratings).\
        map(lambda x: (x[0], (x[1][1][0], x[1][0][0],x[1][1][1],x[1][0][1])))
# id tainias, id xrhsth, titlos, bathmos, dhmotikothta

res3 = genre.join(res2).\
        map(lambda x: (x[0], (x[1][0],x[1][1][0],x[1][1][1],x[1][1][2],x[1][1][3]))).\
        map(lambda x: (x[1][1],(x[1][0],x[0], x[1][2], x[1][3], x[1][4])))
#d xrhsth,eidos , id tainias, titlos, batmos, hmotikothta

res4 = res1.join(res3).\
        map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1][0],x[1][1][1],x[1][1][2],x[1][1][3],x[1][1][4])) if(x[1][0][0]==x[1][1][0]) else ('',('',0,'','','',0.0,0.0)) )
##id xrhsth, eidos, plhthos, eidos tainias, id tainias, titlos, bathmos, dhmotikothta
res5 = res4.\
        map(lambda x: ((x[0], x[1][0]),(x[1][1],x[1][3],x[1][4],x[1][5],x[1][6]))).\
        reduceByKey(lambda x,y: (x[0],x[1],x[2],x[3],x[4]) if(x[3]>y[3] or (x[3]==y[3] and x[4]>y[4])) else (y[0],y[1],y[2],y[3],y[4])).\
        map(lambda x: (x[0][1], (x[0][0], x[1][0],x[1][2],x[1][3])))
##eidos, xrhsths, plhthos,titlos, baumos
res6 = res4.\
        map(lambda x: ((x[0], x[1][0]),(x[1][1],x[1][3],x[1][4],x[1][5],x[1][6]))).\
        reduceByKey(lambda x,y: (x[0],x[1],x[2],x[3],x[4]) if(x[3]<y[3] or (x[3]==y[3] and x[4]>y[4])) else (y[0],y[1],y[2],y[3],y[4])).\
        map(lambda x: (x[0][1], (x[0][0], x[1][0],x[1][2],x[1][3])))
###omoia me res5
res = res5.join(res6).\
        map(lambda x: (x[0],(x[1][0][0],x[1][0][1],x[1][0][2],x[1][0][3],x[1][1][2],x[1][1][3]))).\
        sortByKey(ascending=True).\
        map(lambda x: (x[0],x[1][0],x[1][1],x[1][2],x[1][3],x[1][4],x[1][5])).\
        filter(lambda x: x[0]!='').\
        toDF([ 'Movie_genre','User','Sum_of_ratings','Most_favorite_movie','Rating','Least_favorite_movie', 'Rating'])

#restel = res.intersection(('','',0,'',0.0,'',0.0)).\
#       toDF([ 'Movie_genre','User','Sum_of_ratings','Most_favorite_movie','Rating','Least_favorite_movie', 'Rating'])


######


res.show()

end=time.time()

print("Execution time of query is:")
print(end-start)
