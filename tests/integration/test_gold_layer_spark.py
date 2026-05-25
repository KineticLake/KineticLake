"""
Integration tests for Gold Layer transformations.
Tests actual Spark DataFrame transformations for metrics and spatial calculations.
"""

import pytest
import math
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql import Window


@pytest.mark.integration
class TestGoldKineticMaster:
    """Test Gold layer kinetic master view with actual Spark transformations."""

    @pytest.fixture
    def silver_imu_data(self, spark):
        """Create sample Silver layer IMU data."""
        data = []
        base_time = datetime(2026, 5, 10, 14, 32, 27)

        for i in range(10):
            data.append(
                {
                    "timestamp": base_time + timedelta(seconds=i),
                    "avg_accel_x": 9.8 + i * 0.01,
                    "avg_accel_y": -2.1 + i * 0.005,
                    "avg_gyro_z": 0.05 + i * 0.001,
                }
            )
        return spark.createDataFrame(data)

    @pytest.fixture
    def silver_encoder_data(self, spark):
        """Create sample Silver layer Encoder data."""
        data = []
        base_time = datetime(2026, 5, 10, 14, 32, 27)

        for i in range(10):
            data.append(
                {
                    "timestamp": base_time + timedelta(seconds=i),
                    "total_ticks_per_sec": 1000 + i * 50,
                }
            )
        return spark.createDataFrame(data)

    @pytest.fixture
    def silver_gps_data(self, spark):
        """Create sample Silver layer GPS data."""
        data = []
        base_time = datetime(2026, 5, 10, 14, 32, 27)

        for i in range(10):
            data.append(
                {
                    "timestamp": base_time + timedelta(seconds=i),
                    "latitude": 40.7128 + i * 0.0001,
                    "longitude": -74.0060 + i * 0.0001,
                }
            )
        return spark.createDataFrame(data)

    def test_join_on_timestamp(self, spark, silver_imu_data, silver_encoder_data, silver_gps_data):
        """Test joining silver layers on timestamp."""
        master_df = silver_imu_data.join(silver_encoder_data, on="timestamp", how="left").join(
            silver_gps_data, on="timestamp", how="left"
        )

        assert master_df.count() == 10
        assert "avg_accel_x" in master_df.columns
        assert "total_ticks_per_sec" in master_df.columns
        assert "latitude" in master_df.columns

    def test_velocity_calculation(self, spark, silver_encoder_data):
        """Test velocity calculation from encoder ticks."""
        ticks_to_meters = 0.001745

        velocity_df = silver_encoder_data.withColumn(
            "velocity_mps", F.col("total_ticks_per_sec") * F.lit(ticks_to_meters)
        )

        rows = velocity_df.collect()
        assert all(row["velocity_mps"] > 0 for row in rows)

    def test_acceleration_calculation(self, spark, silver_encoder_data):
        """Test acceleration calculation from velocity changes."""
        ticks_to_meters = 0.001745

        velocity_df = silver_encoder_data.withColumn(
            "velocity_mps", F.col("total_ticks_per_sec") * F.lit(ticks_to_meters)
        )

        window_spec = Window.orderBy("timestamp")

        accel_df = (
            velocity_df.withColumn("prev_velocity", F.lag("velocity_mps", 1).over(window_spec))
            .withColumn("prev_timestamp", F.lag("timestamp", 1).over(window_spec))
            .withColumn(
                "delta_t_sec",
                F.coalesce((F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long")), F.lit(0)),
            )
            .withColumn(
                "acceleration_mps2",
                F.when(
                    F.col("delta_t_sec") > 0, (F.col("velocity_mps") - F.col("prev_velocity")) / F.col("delta_t_sec")
                ).otherwise(F.lit(0.0)),
            )
        )

        # First row should have 0 acceleration
        rows = accel_df.collect()
        assert rows[0]["acceleration_mps2"] == 0.0

    def test_data_quality_checks_timestamp(self, spark, silver_imu_data):
        """Test timestamp not null data quality check."""
        valid_df = silver_imu_data.filter(F.col("timestamp").isNotNull())
        assert valid_df.count() == silver_imu_data.count()

    def test_data_quality_checks_spatial_bounds(self, spark, silver_gps_data):
        """Test spatial bounds data quality checks."""
        valid_df = silver_gps_data.filter((F.abs(F.col("latitude")) <= 90.0) & (F.abs(F.col("longitude")) <= 180.0))
        assert valid_df.count() == silver_gps_data.count()

    def test_data_quality_checks_vibration_safety(self, spark, silver_imu_data):
        """Test chassis vibration safety check."""
        max_vibration = 15.0
        safe_df = silver_imu_data.filter(F.abs(F.col("avg_accel_x")) <= F.lit(max_vibration))
        # All test data should pass
        assert safe_df.count() == silver_imu_data.count()

    def test_data_quality_checks_velocity_safety(self, spark, silver_encoder_data):
        """Test operational velocity safety check."""
        ticks_to_meters = 0.001745
        max_velocity = 45.0

        velocity_df = silver_encoder_data.withColumn(
            "velocity_mps", F.col("total_ticks_per_sec") * F.lit(ticks_to_meters)
        )

        safe_df = velocity_df.filter(F.col("velocity_mps") <= F.lit(max_velocity))
        # All test data should pass
        assert safe_df.count() == velocity_df.count()


@pytest.mark.integration
class TestGoldMLFeatures:
    """Test Gold layer ML feature engineering."""

    @pytest.fixture
    def gold_kinetic_data(self, spark):
        """Create sample gold kinetic data."""
        data = []
        base_time = datetime(2026, 5, 10, 14, 32, 27)

        for i in range(20):
            data.append(
                {
                    "timestamp": base_time + timedelta(seconds=i),
                    "avg_accel_x": 9.8 + i * 0.01,
                    "avg_gyro_z": 0.05 + i * 0.001,
                    "velocity_mps": 1.0 + i * 0.01,
                    "latitude": 40.7128 + i * 0.0001,
                    "longitude": -74.0060 + i * 0.0001,
                }
            )
        return spark.createDataFrame(data)

    def test_sessionization_engine(self, spark, gold_kinetic_data):
        """Test session detection based on time gaps."""
        time_window = Window.orderBy("timestamp")

        session_marked = gold_kinetic_data.withColumn("prev_ts", F.lag("timestamp", 1).over(time_window)).withColumn(
            "is_new_session",
            F.when(F.col("prev_ts").isNull(), F.lit(1))
            .when((F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")) > 5, F.lit(1))
            .otherwise(F.lit(0)),
        )

        # All consecutive seconds should be in same session
        assert session_marked.filter(F.col("is_new_session") == 1).count() == 1  # First row only

    def test_heading_calculation(self, spark, gold_kinetic_data):
        """Test heading calculation from gyro data."""
        window_spec = Window.orderBy("timestamp")

        # Get previous timestamp for dt calculation
        with_prev_ts = gold_kinetic_data.withColumn("prev_ts", F.lag("timestamp", 1).over(window_spec)).withColumn(
            "dt_sec", F.coalesce((F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")), F.lit(1))
        )

        # Calculate heading from gyro
        unbounded_window = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

        heading_df = with_prev_ts.withColumn("gyro_rad_per_sec", F.radians(F.col("avg_gyro_z"))).withColumn(
            "heading_theta", F.sum(F.col("gyro_rad_per_sec") * F.col("dt_sec")).over(unbounded_window)
        )

        rows = heading_df.collect()
        assert len(rows) > 0
        # Heading should be numeric
        assert all(isinstance(row["heading_theta"], (int, float)) for row in rows)

    def test_velocity_decomposition(self, spark, gold_kinetic_data):
        """Test velocity decomposition into x and y components."""
        window_spec = Window.orderBy("timestamp")

        with_heading = (
            gold_kinetic_data.withColumn("prev_ts", F.lag("timestamp", 1).over(window_spec))
            .withColumn(
                "dt_sec", F.coalesce((F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")), F.lit(1))
            )
            .withColumn("heading_theta", F.lit(0.0))  # Simplified for testing
        )

        velocity_components = with_heading.withColumn(
            "delta_x", F.col("velocity_mps") * F.cos(F.col("heading_theta")) * F.col("dt_sec")
        ).withColumn("delta_y", F.col("velocity_mps") * F.sin(F.col("heading_theta")) * F.col("dt_sec"))

        rows = velocity_components.collect()
        assert all(row["delta_x"] > 0 for row in rows[1:])  # Skip first row
        assert all(isinstance(row["delta_y"], (int, float)) for row in rows)

    def test_estimated_path_accumulation(self, spark, gold_kinetic_data):
        """Test estimated path accumulation via dead reckoning."""
        window_spec = Window.orderBy("timestamp")

        # Simple test: heading = 0 (moving in +x direction)
        with_heading = (
            gold_kinetic_data.withColumn("prev_ts", F.lag("timestamp", 1).over(window_spec))
            .withColumn(
                "dt_sec", F.coalesce((F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")), F.lit(1))
            )
            .withColumn("heading_theta", F.lit(0.0))
        )

        delta_df = with_heading.withColumn(
            "delta_x", F.col("velocity_mps") * F.cos(F.col("heading_theta")) * F.col("dt_sec")
        )

        unbounded_window = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

        estimated_path = delta_df.withColumn("estimated_path_x", F.sum(F.col("delta_x")).over(unbounded_window))

        rows = estimated_path.collect()
        # Path should accumulate (each row should be >= previous)
        paths = [row["estimated_path_x"] for row in rows if row["estimated_path_x"] is not None]
        assert all(paths[i] <= paths[i + 1] for i in range(len(paths) - 1))

    def test_actual_path_from_gps(self, spark, gold_kinetic_data):
        """Test actual path calculation from GPS coordinates."""
        unbounded_window = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

        meters_per_degree = 111000.0
        longitude_factor = 0.7

        gps_path = (
            gold_kinetic_data.withColumn("start_latitude", F.first(F.col("latitude")).over(unbounded_window))
            .withColumn("start_longitude", F.first(F.col("longitude")).over(unbounded_window))
            .withColumn("actual_path_y", (F.col("latitude") - F.col("start_latitude")) * F.lit(meters_per_degree))
            .withColumn(
                "actual_path_x",
                (F.col("longitude") - F.col("start_longitude")) * F.lit(meters_per_degree * longitude_factor),
            )
        )

        rows = gps_path.collect()
        assert len(rows) > 0
        # First row should have zero offset
        assert rows[0]["actual_path_y"] == 0.0

    def test_trajectory_drift_calculation(self, spark):
        """Test trajectory drift (distance between estimated and actual paths)."""
        data = [
            {"estimated_path_x": 5.0, "estimated_path_y": 0.0, "actual_path_x": 4.0, "actual_path_y": 3.0},
            {"estimated_path_x": 10.0, "estimated_path_y": 0.0, "actual_path_x": 9.5, "actual_path_y": 2.0},
        ]

        df = spark.createDataFrame(data)

        drift_df = df.withColumn(
            "drift_meters",
            F.sqrt(
                F.pow(F.col("actual_path_x") - F.col("estimated_path_x"), 2)
                + F.pow(F.col("actual_path_y") - F.col("estimated_path_y"), 2)
            ),
        )

        rows = drift_df.collect()
        # First should be sqrt(1 + 9) = sqrt(10)
        assert abs(rows[0]["drift_meters"] - math.sqrt(10)) < 0.01
        # All drifts should be positive
        assert all(row["drift_meters"] >= 0 for row in rows)

    def test_feature_compilation(self, spark, gold_kinetic_data):
        """Test final ML feature compilation."""
        # Simplified version of the full feature pipeline
        features_df = (
            gold_kinetic_data.withColumn("feature_estimated_path_x", F.lit(10.5))
            .withColumn("feature_estimated_path_y", F.lit(2.3))
            .withColumn("feature_actual_path_x", F.lit(10.2))
            .withColumn("feature_actual_path_y", F.lit(2.5))
            .withColumn(
                "target_trajectory_drift_meters",
                F.sqrt(F.pow(F.lit(10.2) - F.lit(10.5), 2) + F.pow(F.lit(2.5) - F.lit(2.3), 2)),
            )
        )

        rows = features_df.collect()
        assert all("feature_estimated_path_x" in row.asDict() for row in rows)
        assert all("target_trajectory_drift_meters" in row.asDict() for row in rows)
        assert all(row["target_trajectory_drift_meters"] >= 0 for row in rows)
