from pyspark.sql.types import *

business_schema = StructType([
    StructField("business_id", StringType()),
    StructField("name", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("categories", StringType())
])


review_schema = StructType([
    StructField("review_id", StringType()),
    StructField("user_id", StringType()),
    StructField("business_id", StringType()),
    StructField("stars", FloatType()),
    StructField("date", StringType()),
    StructField("text", StringType())
])


user_schema = StructType([
    StructField("user_id", StringType()),
    StructField("name", StringType()),
    StructField("yelping_since", StringType()),
    StructField("review_count", IntegerType())
])
