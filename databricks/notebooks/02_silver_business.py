from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from utils.schema_definitions import business_schema


spark = SparkSession.builder \
    .appName("SilverBusiness") \
    .getOrCreate()


bronze = spark.read.table("bronze_yelp_business")


silver = bronze \
    .withColumn(
        "json",
        from_json("raw_json", business_schema)
    ) \
    .select("json.*") \
    .filter(col("business_id").isNotNull()) \
    .dropDuplicates(["business_id"])


silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_business")
