import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum as _sum, when, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, BooleanType, StringType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_transformed")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

def main():
    spark = (
        SparkSession.builder
        .appName("WeatherAggregation")
        .master(SPARK_MASTER)  
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("temp_f", DoubleType(), True),
        StructField("high_wind_alert", BooleanType(), True),
        StructField("time", StringType(), True),
    ])

    # Lecture Kafka en STREAMING
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json")

    parsed = (
        json_df
        .select(from_json(col("json"), schema).alias("data"))
        .select("data.*")
    )

    # time dans tes messages ressemble à "2026-01-22T14:15"
    parsed = parsed.withColumn("event_time", to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm"))

    agg = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"))
        .agg(
            avg("temperature").alias("avg_temp_c"),
            _sum(when(col("high_wind_alert") == True, 1).otherwise(0)).alias("alert_count")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_temp_c"),
            col("alert_count")
        )
    )

    query = (
        agg.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "50")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
