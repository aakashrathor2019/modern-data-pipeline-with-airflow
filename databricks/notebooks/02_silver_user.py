from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from utils.schema_definitions import user_schema


spark = SparkSession.builder \
    .appName("SilverUser") \
    .getOrCreate()


bronze = spark.read.table("bronze_yelp_user")


silver = bronze \
    .withColumn(
        "json",
        from_json("raw_json", user_schema)
    ) \
    .select("json.*") \
    .filter(col("user_id").isNotNull()) \
    .withColumn("yelping_since", to_timestamp("yelping_since")) \
    .dropDuplicates(["user_id"])


silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_user")
