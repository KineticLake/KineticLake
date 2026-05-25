import pyspark.sql.functions as F
from pyspark.sql.functions import col, window, avg
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()


# --- LAYER 1: ENRICHMENT & DISCOVERY ---
@dp.temporary_view(name="enriched_gps_view")
def enriched_gps_view():
    return spark.readStream.table("iot_data.bronze.bronze_gps_data")


# --- LAYER 3: RESAMPLED GPS LOGIC (Streaming) ---
@dp.temporary_view(name="resampled_gps_logic")
def resampled_gps_logic():
    valid_df = spark.readStream.table("enriched_gps_view")

    return (
        valid_df.withWatermark("timestamp", "1 minutes")
        .groupBy(window("timestamp", "1 second"))
        .agg(
            # Averaging coordinates snaps floating network times to a clean 1s point
            avg("latitude").alias("latitude"),
            avg("longitude").alias("longitude"),
        )
        .select(col("window.start").alias("timestamp"), "*")
        .drop("window")
    )


# --- LAYER 4: THE FINAL SILVER SCD2 TABLE ---
dp.create_streaming_table(name="silver_gps_history")

dp.create_auto_cdc_flow(
    target="silver_gps_history",
    source="resampled_gps_logic",
    keys=["timestamp"],
    sequence_by="timestamp",
    stored_as_scd_type=2,
)
