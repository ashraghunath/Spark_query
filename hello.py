import sys
from random import random
from operator import add



# import pandas as pd 

import findspark 
findspark.init('/Users/ashwinraghunath/spark')

import pyspark
from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def parse_file(element):
    return element.split(',')

ratings = sc.textFile("ml-latest-small/ratings.csv")
ratings = ratings.map(parse_file).cache()


movies = sc.textFile("ml-latest-small/movies.csv")
movies = movies.map(parse_file).cache()

links = sc.textFile("ml-latest-small/links.csv")
links = movies.map(parse_file).cache()

tags = sc.textFile("ml-latest-small/tags.csv")
tags = movies.map(parse_file).cache()

ratingsDF = sqlContext.read.csv('ml-latest-small/ratings.csv', header=True, inferSchema=True)
moviesDF = sqlContext.read.csv('ml-latest-small/movies.csv', header=True, inferSchema=True)
tagsDF = sqlContext.read.csv('ml-latest-small/tags.csv', header=True, inferSchema=True)

# 1)
print("1. How many “Drama” movies (movies with the \"Drama\" genre) are there?")
drama_movies = movies.filter(lambda x: x[2].find('Drama') != -1 ).count()
print(drama_movies)
print("\n")

# 2) 
unique_movies_rated = ratings.groupBy(lambda x: x[1]).count()
print("How many unique movies are rated, how many are not rated?")
print("Rated : "+str(unique_movies_rated))

movies_and_ratings = moviesDF.join(ratingsDF, moviesDF.movieId == ratingsDF.movieId).select(moviesDF['movieId'],moviesDF['title'],moviesDF['genres'])
not_rated_count = movies_and_ratings.union(moviesDF).subtract(movies_and_ratings.intersect(moviesDF)).count()
print("Not Rated : "+str(not_rated_count))

print("\n")


# 3)
print("Who gave the most ratings, how many rates did he make?")
user_most_ratings = ratings.groupBy(lambda x: x[0]).map(lambda x: (x[0], x[1].__len__())).max(key=lambda x: x[1])
print(user_most_ratings)
print("\n")

# 4)
print("Compute min, average, max rating per movie.")
print("Average rating: ")
ratingsDF.groupby("movieId").avg("rating").show()

print("Max rating : ")
ratingsDF.groupby("movieId").max("rating").show()

print("Min rating : ")
ratingsDF.groupby("movieId").min("rating").show()

print("\n")


# 5)


# 6)
tagsDF.join(ratingsDF, tagsDF.userId == ratingsDF.userId).select(tagsDF['userId']).distinct().show()
