from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip


# -----------------------------
# Spark + Delta Configuration
# -----------------------------
builder = SparkSession.builder \
    .appName("BronzeLayer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# -----------------------------
# Configs
# -----------------------------
KAFKA_SERVERS = "localhost:9092"
BRONZE_PATH = "/home/my/yelp_data_engineering_project/delta/bronze"


TOPICS = {
    "business": "yelp_business_raw",
    "review": "yelp_review_raw",
    "user": "yelp_user_raw"
}


# -----------------------------
# Kafka Reader
# -----------------------------
def read_kafka_stream(topic_name):

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    return df.selectExpr(
        "CAST(value AS STRING) as raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )


# -----------------------------
# Start Streams
# -----------------------------
for key, topic in TOPICS.items():

    bronze_df = read_kafka_stream(topic)

    bronze_df.writeStream \
        .format("delta") \
        .option(
            "checkpointLocation",
            f"{BRONZE_PATH}/{key}_checkpoint"
        ) \
        .outputMode("append") \
        .start(
            f"{BRONZE_PATH}/bronze_yelp_{key}"
        )


# -----------------------------
# Await
# -----------------------------
spark.streams.awaitAnyTermination()
