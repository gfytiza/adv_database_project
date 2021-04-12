from pyspark.sql import SparkSession
import pandas as pd
import sys
import time

spark = SparkSession.builder.appName("query5-sparkSQL").getOrCreate()



f =sys.argv[1]
if (f == 'csv') :

        movies = spark.read.format('csv').options(header='false',inferSchema='true').load("hdfs://master:9000/exercise/movies.csv")
        genre = spark.read.format('csv').options(header='false', inferSchema='true').load("hdfs://master:9000/exercise/movie_genres.csv")
        ratings = spark.read.format('csv').options(header='false', inferSchema='true').load("hdfs://master:9000/exercise/ratings.csv")
elif ( f == 'parquet') :
        movies = spark.read.format('parquet').options(header='false').load("hdfs://master:9000/exercise/movies.parquet")
        genre = spark.read.format('parquet').options(header='false').load("hdfs://master:9000/exercise/movie_genres.parquet")
        ratings = spark.read.format('parquet').options(header='false').load("hdfs://master:9000/exercise/ratings.parquet")
else:
        raise Exception ("Give a format!")


movies.registerTempTable("movies")
genre.registerTempTable("genre")
ratings.registerTempTable("ratings")

ql1 = \
        "select g._c1 as e, r._c0 as u, count(r._c1) as p "+\
        "from genre as g, ratings as r "+\
        "where g._c0 == r._c1 "+\
        "group by g._c1, r._c0 "

counter = spark.sql(sql1)
counter.registerTempTable("counter")



sql2 = \
        "select s.e as Type, MAX(s.p) as Maximum "+\
        "from counter as s "+\
        "group by s.e "

type_max_critics = spark.sql(sql2)
type_max_critics.registerTempTable("type_max_critics")


sql3 = \
        "select r._c0 as User, COUNT(r._c0) as Count_rat, g._c1 as Type, MAX(r._c2) as Max_rat, MIN(r._c2) as Min_rat "+\
        "from ratings as r, genre as g "+\
        "where r._c1 == g._c0 "+\
        "group by g._c1, r._c0 "

user_max_min = spark.sql(sql3)
user_max_min.registerTempTable("user_max_min")

sql4 = \
        "select t1.Type as Type, t2.User as User, t1.Maximum as Max_count, t2.Max_rat as Max_rat, t2.Min_rat as Min_rat "+\
        "from type_max_critics as t1, user_max_min as t2 "+\
        "where t1.Type == t2.Type and t1.Maximum == t2.Count_rat "+\
        "order by t1.Type "

type_user = spark.sql(sql4)
type_user.registerTempTable("type_user")


sql5 = \
        "select r._c0 as User, g._c1 as Type, m._c1 as Title, r._c2 as Rating, m._c7 as Popularity "+\
        "from movies as m, ratings as r, genre as g "+\
        "where m._c0 == r._c1 and m._c0 == g._c0 "+\
        "order by User desc, Rating desc, Popularity desc "

tainies = spark.sql(sql5)
tainies.registerTempTable("tainies")


sql6 = \
        "select t3.Type as Type, t3.User as User, t3.Max_count as Maximum_critics, FIRST(t4.Title) as Most_favorite, FIRST(t3.Max_rat) as Max_rating, FIRST(t5.Title) as Least_favorite, FIRST(t3.Min_rat) as Min_rating "+\
        "from type_user as t3, tainies as t4, tainies as t5 "+\
        "where t3.User == t4.User and t4.User == t5.User and t3.Type == t4.Type and t4.Type == t5.Type and t4.Rating == t3.Max_rat and t5.Rating == t3.Min_rat "+\
        "group by t3.Type, t3.User, t3.Max_count "

semi_final = spark.sql(sql6)
semi_final.registerTempTable("semi_final")

sqltel = \
        "select t.Type as Genre, first(t.User) as User, first(t.Maximum_critics) as Max_critics, first(Most_favorite) as Favorite_movie, first(Max_rating) as Max_rating, first(Least_favorite) as Least_favorite, first(Min_rating) as Min_rating  "+\
        "from semi_final as t "+\
        "group by t.Type "+\
        "order by t.Type "


start=time.time()

res = spark.sql(sqltel)

res.show()

end=time.time()

print("Execution time of query is:")
print(end-start)
