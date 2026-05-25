from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="bronze_robotics_raw",
    comment="Raw robotics sensor data from source volume"
)
def bronze_robotics_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load("/Volumes/iot_data/iot_data_landing_zone/source")
    )

# IMU Dataframe
@dp.table(
    name="bronze_imu_data",
    comment="Extracted IMU data from the raw robotics data"
)
def bronze_imu_data():
    return (
        spark.readStream.table("bronze_robotics_raw")
        .filter(F.col("sensor") == "IMU")
        .select(
            F.col("sensor").cast("string").alias("sensor"),
            F.col("data.accel_x").cast("double").alias("accel_x"),
            F.col("data.accel_y").cast("double").alias("accel_y"),
            F.col("data.gyro_z").cast("double").alias("gyro_z"),
            F.col("ts").cast("timestamp").alias("timestamp")
        )
    )

# Encoder Dataframe
@dp.table(
    name="bronze_encoder_data",
    comment="Extracted Encoder data from the raw robotics data"
)
def bronze_encoder_data():
    return (
        spark.readStream.table("bronze_robotics_raw")
        .filter(F.col("sensor") == "ENCODER")
        .select(
            F.col("sensor").cast("string").alias("sensor"),
            F.col("data.ticks").cast("long").alias("ticks"),
            F.col("ts").cast("timestamp").alias("timestamp")
        )
    )

# GPS Dataframe
@dp.table(
    name="bronze_gps_data",
    comment="Extracted GPS data from the raw robotics data"
)
def bronze_gps_data():
    return (
        spark.readStream.table("bronze_robotics_raw")
        .filter(F.col("sensor") == "GPS")
        .select(
            F.col("sensor").cast("string").alias("sensor"),
            F.col("data.lat").cast("double").alias("latitude"),
            F.col("data.lon").cast("double").alias("longitude"),
            F.col("ts").cast("timestamp").alias("timestamp")
        )
    )
