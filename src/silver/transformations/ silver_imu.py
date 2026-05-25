import pyspark.sql.functions as F
from pyspark.sql.functions import col, window, avg, stddev, max, min
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()


# --- LAYER 1: ENRICHMENT & DISCOVERY ---
@dp.temporary_view(name="enriched_imu_view")
def enriched_sensors_view():
    # Reading directly from your dedicated Bronze IMU table
    bronze_df = spark.readStream.table("iot_data.bronze.bronze_imu_data")
    return bronze_df


# --- LAYER 3: RESAMPLED MULTI-AXIS LOGIC (Streaming) ---
@dp.temporary_view(name="resampled_imu_logic")
def resampled_silver_logic():
    valid_df = spark.readStream.table("enriched_imu_view")

    return (
        valid_df.withWatermark("timestamp", "1 minutes")
        # Grouping ONLY by the 1-second time window since it's a dedicated table
        .groupBy(window("timestamp", "1 second"))
        .agg(
            # X-Axis Metrics
            avg("accel_x").alias("avg_accel_x"),
            (max("accel_x") - min("accel_x")).alias("spread_accel_x"),
            stddev("accel_x").alias("vibration_index_x"),
            # Y-Axis Metrics
            avg("accel_y").alias("avg_accel_y"),
            (max("accel_y") - min("accel_y")).alias("spread_accel_y"),
            # Z-Axis Gyro Metrics
            avg("gyro_z").alias("avg_gyro_z"),
        )
        # Flatten the window struct back to a standard root timestamp column
        .select(col("window.start").alias("timestamp"), "*")
        .drop("window")
    )


# --- LAYER 4: THE FINAL SILVER SCD2 TABLE ---
dp.create_streaming_table(name="silver_imu_history")

dp.create_auto_cdc_flow(
    target="silver_imu_history",
    source="resampled_imu_logic",
    keys=["timestamp"],  # The timestamp is the unique primary key for this second
    sequence_by="timestamp",
    stored_as_scd_type=2,
)
