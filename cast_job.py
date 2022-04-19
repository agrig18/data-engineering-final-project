from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

app_name = "final_project"

data_lake_path = "hdfs://namenode:8020/data_lake"

root = "/airflow/jobs/"

conf = SparkConf()

hdfs_host = "hdfs://namenode:8020"

conf.set("hive.metastore.uris", "http://hive-metastore:9083")
conf.set("spark.kerberos.access.hadoopFileSystem", hdfs_host)
conf.set("spark.sql.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
conf.set("hive.metastore.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")

spark = SparkSession.builder.appName(app_name).config(conf=conf).getOrCreate()

cast_schema = ArrayType(
    StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )
)

creds = (
    spark.read.option("quote", '"')
    .option("escape", '"')
    .csv(f"{root}credits.csv", header=True)
    .withColumn("cast", f.regexp_replace(f.col("cast"), ": None", ": null"))
    .withColumn("cast", f.regexp_replace(f.col("cast"), "\\\\'", ""))
    .withColumn("cast", f.regexp_replace(f.col("cast"), "\\\\", ""))
    .drop("crew")
)
creds = creds.withColumn("actors", f.from_json(creds.cast, cast_schema))

creds.show()

actors = (
    creds.withColumn("actor", f.explode("actors"))
    .withColumnRenamed("id", "movie_id")
    .select("movie_id", "actor.name")
)

actors.show(20, vertical=False, truncate=False)

cast = actors.withColumn("actor_business_key", f.monotonically_increasing_id()).select(
    "actor_business_key", "movie_id", f.col("name").alias("actor_name")
)

cast.write.option("header", "true").mode("overwrite").csv(f"{data_lake_path}/DimCast.csv")

cast.show()
