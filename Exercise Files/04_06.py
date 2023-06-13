##import required libraries
import pyspark.sql


##create spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config('spark.driver.extraClassPath', "C:/Users/wen_t/Downloads/postgresql-42.6.0.jar") \
    .getOrCreate()

##read movies table from db using spark
movies_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "movies") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()

##add code below
users_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "users") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()


# Use groupBy and mean to aggregate the column
avg_rating = users_df.groupBy("movie_id").mean("rating")

# Join the tables using the film_id column
df = movies_df.join(
    avg_rating,
    movies_df.id==avg_rating.movie_id
)


##print the final dataframe
print(df.show())





