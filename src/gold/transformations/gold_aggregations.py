from pyspark.sql.functions import col, lag, lit, coalesce, when
from pyspark.sql.window import Window
from pyspark import pipelines as dp


def load_dynamic_rules():
    """
    Reads the permanent SQL dimension metadata table and builds a
    Python dictionary compatible with dp.expect_all_or_drop().
    Ex: {"chassis_vibration_safety": "ABS(avg_accel_x) <= 15.0", ...}
    """
    try:
        # Query the permanent metadata table
        rules_df = spark.read.table("iot_data.gold.dim_sensor_metadata")

        # We only want the rules that we can apply to columns present in Gold
        target_axes = ["accel_x", "velocity_mps"]
        filtered_rules = rules_df.filter(col("axis").isin(target_axes)).collect()

        # Map rows into SQL conditional string statements
        rules_dict = {}
        for row in filtered_rules:
            if row["axis"] == "accel_x":
                rules_dict["chassis_vibration_safety"] = f"ABS(avg_accel_x) <= {row['critical_max_threshold']}"
            elif row["axis"] == "velocity_mps":
                rules_dict["operational_velocity_safety"] = f"velocity_mps <= {row['critical_max_threshold']}"

        return rules_dict
    except Exception:
        # Fallback static boundaries to keep CI/CD test runs safe if database is detached
        return {
            "chassis_vibration_safety": "ABS(avg_accel_x) <= 15.0",
            "operational_velocity_safety": "velocity_mps <= 45.0",
        }


# Load the rules map instantly during pipeline initialization
DYNAMIC_RULES = load_dynamic_rules()


def calculate_gold_metrics(imu_df, encoder_df, gps_df):
    """Combines synchronized silver streams and calculates moving kinematics."""
    master_df = imu_df.join(encoder_df, on="timestamp", how="left").join(gps_df, on="timestamp", how="left")

    window_spec = Window.orderBy("timestamp")
    TICKS_TO_METERS = 0.001745

    return (
        master_df.withColumn("velocity_mps", col("total_ticks_per_sec") * lit(TICKS_TO_METERS))
        .withColumn("prev_velocity", lag("velocity_mps", 1).over(window_spec))
        .withColumn("prev_timestamp", lag("timestamp", 1).over(window_spec))
        .withColumn(
            "delta_t_sec", coalesce(col("timestamp").cast("double") - col("prev_timestamp").cast("double"), lit(0.0))
        )
        .withColumn(
            "acceleration_mps2",
            when(col("delta_t_sec") > 0, (col("velocity_mps") - col("prev_velocity")) / col("delta_t_sec")).otherwise(
                lit(0.0)
            ),
        )
        .drop("prev_velocity", "prev_timestamp", "delta_t_sec")
    )


@dp.materialized_view(name="gold_kinetic_master")
# Static Data Quality structural checks
@dp.expect_or_drop("valid_heartbeat_timestamp", "timestamp IS NOT NULL")
@dp.expect_or_drop("valid_spatial_bounds", "ABS(latitude) <= 90.0 AND ABS(longitude) <= 180.0")
# Dynamic Data Quality Checks applied straight from database dictionary!
@dp.expect_all_or_drop(DYNAMIC_RULES)
def gold_kinetic_master():
    # Read and clean Silver tables
    imu = (
        spark.read.table("iot_data.silver.silver_imu_history").filter("__END_AT IS NULL").drop("__START_AT", "__END_AT")
    )
    gps = (
        spark.read.table("iot_data.silver.silver_gps_history").filter("__END_AT IS NULL").drop("__START_AT", "__END_AT")
    )
    encoder = (
        spark.read.table("iot_data.silver.silver_encoder_history")
        .filter("__END_AT IS NULL")
        .drop("__START_AT", "__END_AT")
    )

    return calculate_gold_metrics(imu, encoder, gps)
