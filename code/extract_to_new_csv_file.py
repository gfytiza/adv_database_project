import csv
from csv import reader, writer

data = list(reader(open("movie_genres.csv", "r"), delimiter=","))

out = writer(open("movie_genres_100.csv", "w"), delimiter=",")
#csvwriter = csv.writer(out) 
for i in range(0,100):
    out.writerow(data[i])
