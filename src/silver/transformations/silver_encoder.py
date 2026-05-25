import pyspark.sql.functions as F
from pyspark.sql.functions import col, window, sum, max, avg
from pyspark import pipelines as dp

# --- LAYER 1: ENRICHMENT & DISCOVERY ---
@dp.temporary_view(name="enriched_encoder_view")
def enriched_encoder_view():
    return spark.readStream.table("iot_data.bronze.bronze_encoder_data")

# --- LAYER 3: RESAMPLED ENCODER LOGIC (Streaming) ---
@dp.temporary_view(name="resampled_encoder_logic")
def resampled_encoder_logic():
    valid_df = spark.readStream.table("enriched_encoder_view")
    
    return (
        valid_df
        .withWatermark("timestamp", "1 minutes")
        .groupBy(window("timestamp", "1 second"))
        .agg(
            # Total distance/ticks accumulated in this 1-second window
            sum("ticks").alias("total_ticks_per_sec"),
            # Peak speed metric observed within this second
            #max("velocity").alias("peak_velocity")
        )
        .select(col("window.start").alias("timestamp"), "*")
        .drop("window")
    )

# --- LAYER 4: THE FINAL SILVER SCD2 TABLE ---
dp.create_streaming_table(name="silver_encoder_history")

dp.create_auto_cdc_flow(
    target="silver_encoder_history",
    source="resampled_encoder_logic",
    keys=["timestamp"],
    sequence_by="timestamp",           
    stored_as_scd_type=2 
)