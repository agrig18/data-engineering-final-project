from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from pyspark.sql.types import *
from pyspark.sql.functions import udf

from imdb import IMDb

import sys

max_size = 10

if len(sys.argv) != 0:
    i = int(sys.argv[1])
    num_threads = int(sys.argv[2])
else:
    sys.exit(-1)

                    
app_name = 'final_project'

data_lake_path = 'hdfs://namenode:8020/data_lake'

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

def save_poster(movie_id: str) -> str:
    ia = IMDb()
    movie = ia.get_movie(movie_id)

    return movie['cover url']

movies = spark.read\
              .option("header", "true")\
              .csv(f'{data_lake_path}/FactMovies.csv')\
              .limit(max_size)\
              .withColumn('row_index', f.monotonically_increasing_id())

num_task_per_process = movies.count()/num_threads

movies = movies.where((f.col("row_index") >= i * num_task_per_process) & (f.col("row_index") < (i+1) * num_task_per_process))


get_poster = udf(
        lambda val: save_poster(val),
        StringType()
)

movies.show()

movies.select('imdb_id').show()

poster_urls = movies.withColumn('poster_url', get_poster(movies.imdb_id))

poster_urls.show()


poster_urls.write.option("header", "true").mode('overwrite').csv(f'{data_lake_path}/DimPosterUrl{i}.csv')\

spark.read.option("header", "true").csv(f"{data_lake_path}/DimPosterUrl{i}.csv").show()