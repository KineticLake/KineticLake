import pyspark.sql.functions as F
from pyspark.sql.functions import col, lag, lit, when, coalesce
from pyspark.sql.window import Window
from pyspark import pipelines as dp

def generate_spatial_features(gold_df):
    """
    Advanced Spatial Feature Engine.
    Safely creates simulation session partitions and integrates physics over exact time deltas.
    """
    # -------------------------------------------------------------------------
    # 1. SESSIONIZATION ENGINE (Detecting Simulation Restarts safely)
    # -------------------------------------------------------------------------
    time_window = Window.orderBy("timestamp")
    
    session_marked_df = (
        gold_df
        .withColumn("prev_ts", lag("timestamp", 1).over(time_window))
        .withColumn(
            "is_new_session",
            # Ensure the very first row safely starts Session 1
            when(col("prev_ts").isNull(), lit(1))
            # Any gap > 5 seconds triggers a new session partition
            .when(col("timestamp").cast("double") - col("prev_ts").cast("double") > 5.0, lit(1))
            .otherwise(lit(0))
        )
    )
    
    session_id_df = (
        session_marked_df
        .withColumn("session_id", F.sum("is_new_session").over(time_window))
        .drop("is_new_session")
    )

    # -------------------------------------------------------------------------
    # 2. ISOLATED PHYSICS WINDOWS (Partitioned by Session)
    # -------------------------------------------------------------------------
    chronological_window = Window.partitionBy("session_id").orderBy("timestamp")
    unbounded_window = (
        Window.partitionBy("session_id")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    METERS_PER_DEGREE = 111000.0
    
    # Calculate exact delta-t for integration
    time_df = session_id_df.withColumn(
        "dt_sec", 
        coalesce(col("timestamp").cast("double") - col("prev_ts").cast("double"), lit(0.0))
    )

    # -------------------------------------------------------------------------
    # 3. SPATIAL MATHEMATICS (Dead Reckoning & Mapping)
    # -------------------------------------------------------------------------
    spatial_math_df = (
        time_df
        # Assuming IMU is in degrees; convert to Radians. Remove F.radians if already radians.
        .withColumn("gyro_rad_per_sec", F.radians(col("avg_gyro_z"))) 
        
        # Integrate yaw rate over exact time elapsed to get current heading
        .withColumn("heading_theta", F.sum(col("gyro_rad_per_sec") * col("dt_sec")).over(unbounded_window))
        
        # Calculate localized movement vectors using exact time step
        .withColumn("delta_x", col("velocity_mps") * F.cos(col("heading_theta")) * col("dt_sec"))
        .withColumn("delta_y", col("velocity_mps") * F.sin(col("heading_theta")) * col("dt_sec"))
    )
    
    # -------------------------------------------------------------------------
    # 4. FINAL ML FEATURE COMPILATION
    # -------------------------------------------------------------------------
    return (
        spatial_math_df
        # Features 1 & 2: Estimated continuous trajectory (Dead Reckoning)
        .withColumn("feature_estimated_path_x", F.sum("delta_x").over(unbounded_window))
        .withColumn("feature_estimated_path_y", F.sum("delta_y").over(unbounded_window))
        
        # Features 3 & 4: Actual GPS mapping relative to the session's start position
        .withColumn("feature_actual_path_x", (col("latitude") - F.first("latitude").over(unbounded_window)) * lit(METERS_PER_DEGREE))
        .withColumn("feature_actual_path_y", (col("longitude") - F.first("longitude").over(unbounded_window)) * lit(METERS_PER_DEGREE * 0.7))
        
        # Target Label: Euclidean distance representing sensor drift error
        .withColumn(
            "target_trajectory_drift_meters",
            F.sqrt(
                F.pow(col("feature_actual_path_x") - col("feature_estimated_path_x"), 2) +
                F.pow(col("feature_actual_path_y") - col("feature_estimated_path_y"), 2)
            )
        )
        .drop("prev_ts", "dt_sec", "gyro_rad_per_sec", "heading_theta", "delta_x", "delta_y")
    )

@dp.materialized_view(name="gold_ml_features")
def gold_ml_features():
    # Read natively from the upstream live view generated in Script 1
    upstream_gold_data = spark.read.table("live.gold_kinetic_master")
    return generate_spatial_features(upstream_gold_data)