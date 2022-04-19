from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

app_name = 'final_project'

data_lake_path = 'hdfs://namenode:8020/data_lake'

root = '/airflow/jobs/'


conf = SparkConf()

hdfs_host = 'hdfs://namenode:8020'

conf.set("hive.metastore.uris", "http://hive-metastore:9083")
conf.set("spark.kerberos.access.hadoopFileSystem", hdfs_host)
conf.set("spark.sql.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
conf.set("hive.metastore.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")

spark = SparkSession\
  .builder\
  .appName(app_name)\
  .config(conf=conf)\
  .getOrCreate()


movies_meta = spark\
  .read\
  .option("multiLine", "true")\
  .option("quote", '"')\
  .option("header", "true")\
  .option("escape", '"')\
  .option("wholeFile", True)\
  .csv(f"{root}movies_metadata.csv", header=True)\

movies_meta = movies_meta\
  .where("imdb_id not in ('0', 'tt0113002', 'tt2423504', 'tt2622826')")

movies = movies_meta\
  .withColumn('imdb_id', f.col('imdb_id').substr(3, 100))\
  .select('id', 'title', 'imdb_id', 'original_title', 'overview', 'homepage', 'popularity', 
          'release_date', 'revenue', 'runtime', 'status')

# recalculate vote_count and vote_average and join them with movies

ratings = spark\
        .read\
        .option('inferSchema', 'true')\
        .option("wholeFile", True)\
        .csv(f"{root}ratings.csv", header=True)\
        .drop('timestamp', 'userId')
        
        
ratings = ratings.groupBy('movieId')\
                 .agg(f.count('*').alias('vote_count'),\
                      f.avg('rating').cast('decimal(10, 1)').alias('vote_average'))\

movies = movies\
                .join(ratings, f.col('id') == f.col('movieId'), 'left')\
                .drop(f.col('movieId'))\

movies.write.option("header", "true").mode('overwrite').csv(f'{data_lake_path}/FactMovies.csv')

movies.show()

# Keywords

keywords = spark\
          .read\
          .option("multiLine", "true")\
          .option("quote", '"')\
          .option("header", "true")\
          .option("escape", '"')\
          .option("wholeFile", True)\
          .csv(f"{root}keywords.csv", header=True)

keywords_schema = ArrayType(StructType([StructField('id',IntegerType(),True),
                        StructField('name',StringType(),True)]))

keywords = keywords\
          .withColumn('keyword', f.from_json(f.col('keywords'), keywords_schema))\
          .withColumn('keyword', f.explode(f.col("keyword.name")))\
          .withColumnRenamed('id', 'movie_id')\
          .withColumn('keyword_business_key', f.monotonically_increasing_id())\
          .select('keyword_business_key', 'keyword', 'movie_id')

keywords.write.option("header", "true").mode('overwrite').csv(f'{data_lake_path}/DimKeywords.csv')

keywords.show()

# Genres

genre_schema = ArrayType(StructType([StructField('name',StringType(),True)]))

genres = movies_meta\
      .withColumn('genres', f.regexp_replace(f.col('genres'), ': None', ': null'))\
      .withColumn('genres', f.regexp_replace(f.col('genres'), "\\\\'", ""))\
      .withColumn('genres', f.regexp_replace(f.col('genres'), "\\\\", ""))\
      .withColumnRenamed('id', 'movie_id')\
      .select('movie_id', 'genres')

genres = genres\
  .withColumn('genres', f.from_json(genres.genres, genre_schema))

genres.show()

genres = genres\
  .withColumn('genre', f.explode('genres'))\
  .select('movie_id','genre.name')

genres.show(20, vertical=False, truncate=False)

genres = genres\
    .withColumn('genre_business_key', f.monotonically_increasing_id())\
    .withColumnRenamed('name', 'genre')\
    .select('genre_business_key', 'genre', 'movie_id')

genres.write.option("header", "true").mode('overwrite').csv(f'{data_lake_path}/DimGenre.csv')

genres.show()

# Languages

language_schema = ArrayType(StructType([StructField('name',StringType(),True)]))

languages = movies_meta\
      .withColumn('spoken_languages', f.regexp_replace(f.col('spoken_languages'), ': None', ': null'))\
      .withColumn('spoken_languages', f.regexp_replace(f.col('spoken_languages'), "\\\\'", ""))\
      .withColumn('spoken_languages', f.regexp_replace(f.col('spoken_languages'), "\\\\", ""))\
      .withColumnRenamed('id', 'movie_id')\
      .withColumnRenamed('spoken_languages', 'languages')\
      .select('movie_id', 'languages')

languages = languages\
  .withColumn('languages', f.from_json(languages.languages, language_schema))

languages.show()

languages = languages\
  .withColumn('languages', f.explode('languages'))\
  .select('movie_id', 'languages.name')

languages.show(20, vertical=False, truncate=False)

languages = languages\
    .withColumn('language_business_key', f.monotonically_increasing_id())\
    .withColumnRenamed('name', 'language')\
    .select('language_business_key', 'language', 'movie_id')

languages.write.option("header", "true").mode('overwrite').csv(f'{data_lake_path}/DimLanguage.csv')

languages.show()

# Production Companies

companies_schema = ArrayType(StructType([StructField('name',StringType(),True)]))

production_companies = movies_meta\
    .withColumn('company', f.from_json(f.col('production_companies'), companies_schema))\
    .withColumn("company", f.explode('company.name'))\
    .withColumn('prod_company_business_key', f.monotonically_increasing_id())\
    .withColumnRenamed('id', 'movie_id')\
    .select('prod_company_business_key', 'company', 'movie_id')

production_companies.write.option("header", "true").mode('overwrite').csv(f'{data_lake_path}/DimProdCompanies.csv')

production_companies.show()

# Production Countries

countries_schema = ArrayType(StructType([StructField('name',StringType(),True)]))

production_countries = movies_meta\
    .withColumn('country', f.from_json(f.col('production_countries'), countries_schema))\
    .withColumn("country", f.explode('country.name'))\
    .withColumn('prod_country_business_key', f.monotonically_increasing_id())\
    .withColumnRenamed('id', 'movie_id')\
    .select('prod_country_business_key','country', 'movie_id')

production_countries.write.option("header", "true").mode('overwrite').csv(f'{data_lake_path}/DimProdCountries.csv')

production_countries.show()