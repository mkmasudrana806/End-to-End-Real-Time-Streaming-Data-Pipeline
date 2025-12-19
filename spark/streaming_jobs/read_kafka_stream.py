from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum as spark_sum, round as spark_round
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("KafkaStreamAnalysis")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# defien the schema for the incoming kafka messages
schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("event_time", StringType())
])

# read the Kafka raw stream
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "user_events")
    .option("startingOffsets", "earliest")
    .load()
)

# parse the JSON messages and extract fields
parsed_df = (
    raw_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)



# event_time as timestamp and watermarking
base_df = (
    parsed_df
    .withColumn("event_time_ts", to_timestamp("event_time"))
    .withWatermark("event_time_ts", "2 minutes")
)


def write_to_postgres(df, epoch_id, table_name):
    (
        df.write
        .mode("append")
        .jdbc(
            url="jdbc:postgresql://postgres:5432/airflow",
            table=table_name,
            properties={
                "user": "airflow",
                "password": "airflow",
                "driver": "org.postgresql.Driver"
            }
        )
    )


# ========== product view window per 30 minutes ============
# which products are getting the most views

product_view_df = (
    base_df
    .filter(col("event_type") == "product_view")
    .groupBy(
        window(col("event_time_ts"), "30 minutes"),
        col("product_id")
    )
    .count()
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product_id"),
        col("count").alias("view_count")
    )
)

# uncomment it to see console output
# product_view_query = (
#     product_view_df
#     .writeStream
#     .outputMode("append")
#     .format("console")
#     .option("truncate", "false")
#     .queryName("product_view_window")
#     .start()
# )


product_view_query = (
    product_view_df
    .writeStream
    .foreachBatch(lambda df, epoch: write_to_postgres(df, epoch, "product_view_window"))
    .outputMode("append")
    .start()
)



# ========= category activity window per 5 minutes ==========
# which category is most active based on all event types

category_activity_df = (
    base_df
    .groupBy(
        window(col("event_time_ts"), "5 minutes"),
        col("category")
    )
    .count()
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("count").alias("event_count")
    )
)

# uncomment it to see console output
# category_activity_query = (
#     category_activity_df
#     .writeStream
#     .outputMode("append")
#     .format("console")
#     .option("truncate", "false")
#     .queryName("category_activity_window")
#     .start()
# )

category_activity_query = (
    category_activity_df
    .writeStream
    .foreachBatch(lambda df, epoch: write_to_postgres(df, epoch, "category_activity_window"))
    .outputMode("append")
    .start()
)


# ======= category revenue window (5 minutes) ============
# how much revenue generated per category

category_revenue_df = (
    base_df
    .filter(col("event_type") == "purchase")
    .groupBy(
        window(col("event_time_ts"), "5 minutes"),
        col("category")
    )
    .agg(
        spark_round(spark_sum("price"), 2).alias("total_revenue")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("total_revenue")
    )
)

# uncomment it to see console output
# category_revenue_query = (
#     category_revenue_df
#     .writeStream
#     .outputMode("append")
#     .format("console")
#     .option("truncate", "false")
#     .queryName("category_revenue_window")
#     .start()
# )

category_revenue_query = (
    category_revenue_df
    .writeStream
    .foreachBatch(lambda df, epoch: write_to_postgres(df, epoch, "category_revenue_window"))
    .outputMode("append")
    .start()
)


spark.streams.awaitAnyTermination()

# app run command: docker exec -it spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.postgresql:postgresql:42.7.3 /opt/spark-apps/read_kafka_stream.py