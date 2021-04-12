import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csvToparquet").getOrCreate()

ratingDF = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/exercise/ratings.csv")


ratingDF.write.parquet("hdfs://master:9000/exercise/ratings.parquet")

moviesDF = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/exercise/movies.csv")


moviesDF.write.parquet("hdfs://master:9000/exercise/movies.parquet")

genreDF = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/exercise/movie_genres.csv")


genreDF.write.parquet("hdfs://master:9000/exercise/movie_genres.parquet")