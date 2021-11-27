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

from pyspark.sql.functions import split,explode,regexp_extract

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
print("2. How many unique movies are rated, how many are not rated?")
print("Rated : "+str(unique_movies_rated))

movies_and_ratings = moviesDF.join(ratingsDF, moviesDF.movieId == ratingsDF.movieId).select(moviesDF['movieId'],moviesDF['title'],moviesDF['genres'])
not_rated_count = movies_and_ratings.union(moviesDF).subtract(movies_and_ratings.intersect(moviesDF)).count()
print("Not Rated : "+str(not_rated_count))

print("\n")


# 3)
print("3. Who gave the most ratings, how many rates did he make?")
user_most_ratings = ratings.groupBy(lambda x: x[0]).map(lambda x: (x[0], x[1].__len__())).max(key=lambda x: x[1])
print(user_most_ratings)
print("\n")

# 4)
print("4. Compute min, average, max rating per movie.")
print("Average Rating, Max Rating and  Min Rating")
average_rating = ratingsDF.groupby("movieId").avg("rating")
max_rating = ratingsDF.groupby("movieId").max("rating")
min_rating = ratingsDF.groupby("movieId").min("rating")
print("\n")
average_rating.join(max_rating, "movieId").join(min_rating, "movieId").show()

# 6)
print("6. Output dataset containing users that have rated AND tagged a movie.")
rated_and_tagged = tagsDF.join(ratingsDF, tagsDF.userId == ratingsDF.userId).select(tagsDF['userId']).distinct()
rated_and_tagged.show()

# 5)
print("5. Output dataset containing users that have rated a movie but not tagged it.")
uniontable = ratingsDF.select(ratingsDF['userId']).union(rated_and_tagged).distinct()
uniontable.subtract(rated_and_tagged.intersect(uniontable)).show()


split_genres = moviesDF.withColumn("genres", explode(split("genres","[|]")))
#count of movies per genre
split_genres.groupBy('genres').count().show()
dramaCount = split_genres.filter(split_genres.genres=="Drama").count()
print(dramaCount)

dc = movies.filter(lambda x: x[2].count('Drama')!=0).count()
print(dc)


movies_with_year_df = moviesDF.select('movieId','title',regexp_extract('title',r'\((\d+)\)',1).alias('year'))
movies_with_year_df.show()
movies_with_year_df.groupBy('year').count().show()


genres_and_years = split_genres.join(movies_with_year_df, "movieId")
genres_and_years.groupBy('genres','year').count().show()
genres_and_years.groupBy('year').count().show()
# moviesDF.groupBy('genres').count().show()