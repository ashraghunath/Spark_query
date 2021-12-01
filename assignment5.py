import findspark 
findspark.init('/Users/ashwinraghunath/spark')

#Import statements
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode,regexp_extract

spark = SparkSession.builder.appName("DSD Assignment 5").getOrCreate()


#RDD
moviesRDD = spark.read.csv("ml-latest-small/movies.csv",header=True).rdd
ratingsRDD = spark.read.csv("ml-latest-small/ratings.csv",header=True).rdd


ratingsDF = spark.read.csv('ml-latest-small/ratings.csv', header=True, inferSchema=True)
ratingsRDD = spark.read.csv('ml-latest-small/ratings.csv', header=True, inferSchema=True).rdd
moviesDF = spark.read.csv('ml-latest-small/movies.csv', header=True, inferSchema=True)
tagsDF = spark.read.csv('ml-latest-small/tags.csv', header=True, inferSchema=True)


print("\n\n")
#Assignment 5 queries
# 1)
print("1. How many “Drama” movies (movies with the \"Drama\" genre) are there?")
drama_movies = moviesRDD.filter(lambda x:x[2].count('Drama')!=0).count()
print(drama_movies)
print("\n")

# 2) 
unique_movies_rated = ratingsRDD.groupBy(lambda x: x[1]).count()
print("2. How many unique movies are rated, how many are not rated?")
print("Rated : "+str(unique_movies_rated))

movies_and_ratings = moviesDF.join(ratingsDF, "movieId").select(moviesDF['movieId'],moviesDF['title'],moviesDF['genres'])
not_rated_count = moviesDF.subtract(movies_and_ratings).sort('movieId').count()
print("Not Rated : "+str(not_rated_count))
print("\n")

# 3)
print("3. Who gave the most ratings, how many rates did he make?")
user_most_ratings = ratingsRDD.groupBy(lambda x: x[0]).map(lambda x: (x[0], x[1].__len__())).max(key=lambda x: x[1])
print(user_most_ratings)
print("\n")

# 4)
print("4. Compute min, average, max rating per movie.")
print("Average Rating, Max Rating and  Min Rating")
average_rating = ratingsDF.groupby("movieId").avg("rating")
max_rating = ratingsDF.groupby("movieId").max("rating")
min_rating = ratingsDF.groupby("movieId").min("rating")
average_rating.join(max_rating, "movieId").join(min_rating, "movieId").show()
print("\n")

# 5)
print("5. Output dataset containing users that have rated a movie but not tagged it.")
rated_and_tagged = tagsDF.join(ratingsDF, "userId").select(tagsDF['userId']).distinct()
rated_not_tagged = ratingsDF.select(ratingsDF['userId']).subtract(rated_and_tagged)
rated_not_tagged.sort('userId').show(rated_not_tagged.count())

# 6)
print("6. Output dataset containing users that have rated AND tagged a movie.")
rated_and_tagged.sort('userId').show(rated_and_tagged.count())

# 7)
individual_genres = moviesDF.withColumn("genres", explode(split("genres","[|]")))
movies_with_year = moviesDF.select('movieId','title',regexp_extract('title',r'\((\d+)\)',1).alias('year'))
genres_and_years = individual_genres.join(movies_with_year, "movieId")
print("7. Output dataset showing the number of movies per Genre per Year (movies will be counted many times if it's associated with multiple genres).")
genres_and_years.groupBy('genres','year').count().show()



