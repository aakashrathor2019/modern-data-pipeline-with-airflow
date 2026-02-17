from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from utils.schema_definitions import review_schema


spark = SparkSession.builder \
    .appName("SilverReview") \
    .getOrCreate()


bronze = spark.read.table("bronze_yelp_review")


silver = bronze \
    .withColumn(
        "json",
        from_json("raw_json", review_schema)
    ) \
    .select("json.*") \
    .filter(col("review_id").isNotNull()) \
    .withColumn("date", to_timestamp("date")) \
    .dropDuplicates(["review_id"])


silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_review")
