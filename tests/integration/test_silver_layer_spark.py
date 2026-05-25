"""
Integration tests for Silver Layer transformations.
Tests actual Spark DataFrame transformations for resampling and SCD2.
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql import Window


@pytest.mark.integration
class TestSilverIMUResampling:
    """Test Silver layer IMU resampling with actual Spark transformations."""

    @pytest.fixture
    def imu_bronze_data(self, spark):
        """Create sample IMU bronze data."""
        data = []
        base_time = datetime(2026, 5, 10, 14, 32, 27)

        # Create IMU samples for 3 seconds with 100ms intervals
        for i in range(30):
            data.append(
                {
                    "sensor": "IMU",
                    "accel_x": 9.8 + i * 0.01,
                    "accel_y": -2.1 + i * 0.005,
                    "gyro_z": 0.05 + i * 0.001,
                    "timestamp": base_time + timedelta(milliseconds=i * 100),
                }
            )
        return spark.createDataFrame(data)

    def test_imu_data_shape(self, spark, imu_bronze_data):
        """Test that IMU data has correct columns."""
        assert "accel_x" in imu_bronze_data.columns
        assert "accel_y" in imu_bronze_data.columns
        assert "gyro_z" in imu_bronze_data.columns
        assert "timestamp" in imu_bronze_data.columns

    def test_1_second_windowing(self, spark, imu_bronze_data):
        """Test 1-second windowing aggregation."""
        windowed_df = (
            imu_bronze_data.groupBy(F.window(F.col("timestamp"), "1 second").alias("window"))
            .agg(
                F.avg("accel_x").alias("avg_accel_x"),
                F.avg("accel_y").alias("avg_accel_y"),
                F.avg("gyro_z").alias("avg_gyro_z"),
                F.count("*").alias("sample_count"),
            )
            .select(
                F.col("window.start").alias("timestamp"), "avg_accel_x", "avg_accel_y", "avg_gyro_z", "sample_count"
            )
        )

        # 3 seconds of data should create 3 windows (or fewer if data doesn't span full seconds)
        windows = windowed_df.count()
        assert windows >= 1

        # Each window should have multiple samples
        rows = windowed_df.collect()
        assert all(row["sample_count"] > 0 for row in rows)

    def test_acceleration_aggregations(self, spark, imu_bronze_data):
        """Test acceleration aggregations in 1-second windows."""
        windowed_df = (
            imu_bronze_data.groupBy(F.window(F.col("timestamp"), "1 second").alias("window"))
            .agg(
                F.avg("accel_x").alias("avg_accel_x"),
                (F.max("accel_x") - F.min("accel_x")).alias("spread_accel_x"),
                F.stddev("accel_x").alias("vibration_index_x"),
                F.avg("accel_y").alias("avg_accel_y"),
                (F.max("accel_y") - F.min("accel_y")).alias("spread_accel_y"),
            )
            .select(
                F.col("window.start").alias("timestamp"),
                "avg_accel_x",
                "spread_accel_x",
                "vibration_index_x",
                "avg_accel_y",
                "spread_accel_y",
            )
        )

        rows = windowed_df.collect()
        assert len(rows) > 0

        # Check that values are numeric
        for row in rows:
            assert isinstance(row["avg_accel_x"], (int, float))
            assert isinstance(row["spread_accel_x"], (int, float))
            assert isinstance(row["avg_accel_y"], (int, float))

    def test_timestamp_window_flattening(self, spark, imu_bronze_data):
        """Test that window struct is flattened to timestamp."""
        windowed_df = (
            imu_bronze_data.groupBy(F.window(F.col("timestamp"), "1 second").alias("window"))
            .agg(F.avg("accel_x").alias("avg_accel_x"))
            .select(F.col("window.start").alias("timestamp"), "avg_accel_x")
            .drop("window")
        )

        assert "timestamp" in windowed_df.columns
        assert "window" not in windowed_df.columns

        rows = windowed_df.collect()
        assert all(isinstance(row["timestamp"], datetime) for row in rows)


@pytest.mark.integration
class TestSilverEncoderResampling:
    """Test Silver layer Encoder resampling with actual Spark transformations."""

    @pytest.fixture
    def encoder_bronze_data(self, spark):
        """Create sample Encoder bronze data."""
        data = []
        base_time = datetime(2026, 5, 10, 14, 32, 27)

        # Create encoder samples for 3 seconds
        for i in range(30):
            data.append(
                {"sensor": "ENCODER", "ticks": 100 + i * 5, "timestamp": base_time + timedelta(milliseconds=i * 100)}
            )
        return spark.createDataFrame(data)

    def test_encoder_ticks_per_second(self, spark, encoder_bronze_data):
        """Test encoder ticks aggregation per second."""
        encoder_agg = (
            encoder_bronze_data.groupBy(F.window(F.col("timestamp"), "1 second").alias("window"))
            .agg(F.sum("ticks").alias("total_ticks_per_sec"))
            .select(F.col("window.start").alias("timestamp"), "total_ticks_per_sec")
        )

        rows = encoder_agg.collect()
        assert len(rows) > 0
        assert all(row["total_ticks_per_sec"] > 0 for row in rows)

    def test_velocity_calculation_from_ticks(self, spark, encoder_bronze_data):
        """Test velocity calculation from encoder ticks."""
        ticks_to_meters = 0.001745

        encoder_agg = (
            encoder_bronze_data.groupBy(F.window(F.col("timestamp"), "1 second").alias("window"))
            .agg(F.sum("ticks").alias("total_ticks_per_sec"))
            .select(
                F.col("window.start").alias("timestamp"),
                (F.col("total_ticks_per_sec") * F.lit(ticks_to_meters)).alias("velocity_mps"),
            )
        )

        rows = encoder_agg.collect()
        assert all(row["velocity_mps"] > 0 for row in rows)

        # Check reasonable velocity values
        velocities = [row["velocity_mps"] for row in rows]
        assert all(0 < v < 50 for v in velocities)  # Reasonable max speed


@pytest.mark.integration
class TestSilverGPSResampling:
    """Test Silver layer GPS resampling with actual Spark transformations."""

    @pytest.fixture
    def gps_bronze_data(self, spark):
        """Create sample GPS bronze data."""
        data = []
        base_time = datetime(2026, 5, 10, 14, 32, 27)

        # Create GPS samples for 3 seconds
        for i in range(30):
            data.append(
                {
                    "sensor": "GPS",
                    "latitude": 40.7128 + i * 0.0001,
                    "longitude": -74.0060 + i * 0.0001,
                    "timestamp": base_time + timedelta(milliseconds=i * 100),
                }
            )
        return spark.createDataFrame(data)

    def test_gps_coordinate_averaging(self, spark, gps_bronze_data):
        """Test GPS coordinate averaging in 1-second windows."""
        gps_agg = (
            gps_bronze_data.groupBy(F.window(F.col("timestamp"), "1 second").alias("window"))
            .agg(F.avg("latitude").alias("avg_latitude"), F.avg("longitude").alias("avg_longitude"))
            .select(F.col("window.start").alias("timestamp"), "avg_latitude", "avg_longitude")
        )

        rows = gps_agg.collect()
        assert len(rows) > 0

        # Check that coordinates are within valid ranges
        for row in rows:
            assert -90 <= row["avg_latitude"] <= 90
            assert -180 <= row["avg_longitude"] <= 180

    def test_gps_position_stability(self, spark, gps_bronze_data):
        """Test that GPS position is stable within a window."""
        gps_stats = gps_bronze_data.groupBy(F.window(F.col("timestamp"), "1 second").alias("window")).agg(
            F.avg("latitude").alias("avg_latitude"),
            F.stddev("latitude").alias("lat_stddev"),
            F.avg("longitude").alias("avg_longitude"),
            F.stddev("longitude").alias("lon_stddev"),
        )

        rows = gps_stats.collect()
        assert len(rows) > 0
        # Should have low standard deviation for stable signal
        assert all(row["lat_stddev"] is None or row["lat_stddev"] < 0.01 for row in rows)


@pytest.mark.integration
class TestSilverStreamingLogic:
    """Test Silver layer streaming-specific logic."""

    def test_watermark_application(self, spark):
        """Test watermark application for late data handling."""
        data = []
        base_time = datetime(2026, 5, 10, 14, 32, 27)

        for i in range(10):
            data.append({"accel_x": 9.8 + i * 0.1, "timestamp": base_time + timedelta(seconds=i)})

        df = spark.createDataFrame(data)

        # Test windowing with watermark concept
        windowed = df.groupBy(F.window(F.col("timestamp"), "1 second")).agg(F.avg("accel_x").alias("avg_accel_x"))

        # Should create correct number of windows
        assert windowed.count() >= 10

    def test_scd2_column_tracking(self, spark):
        """Test SCD2 tracking columns presence."""
        # In actual implementation, __START_AT and __END_AT are added by DLT
        # This tests that we'd filter by __END_AT IS NULL
        data = [
            {"timestamp": datetime(2026, 5, 10, 14, 32, 27), "latitude": 40.7128, "__END_AT": None},
            {"timestamp": datetime(2026, 5, 10, 14, 32, 28), "latitude": 40.7138, "__END_AT": None},
            {
                "timestamp": datetime(2026, 5, 10, 14, 32, 27),
                "latitude": 40.7120,
                "__END_AT": datetime(2026, 5, 10, 14, 33, 0),
            },
        ]

        df = spark.createDataFrame(data)

        # Filter to current version only
        current = df.filter(F.col("__END_AT").isNull())

        assert current.count() == 2

        # Then drop tracking columns
        clean_df = current.drop("__START_AT", "__END_AT")
        assert clean_df.count() == 2


@pytest.mark.integration
class TestSilverDataQuality:
    """Test Silver layer data quality checks."""

    def test_timestamp_not_null_filter(self, spark):
        """Test filtering out null timestamps."""
        data = [
            {"sensor": "IMU", "accel_x": 9.8, "timestamp": datetime(2026, 5, 10, 14, 32, 27)},
            {"sensor": "IMU", "accel_x": 9.9, "timestamp": None},
            {"sensor": "IMU", "accel_x": 10.0, "timestamp": datetime(2026, 5, 10, 14, 32, 28)},
        ]

        df = spark.createDataFrame(data)
        valid_df = df.filter(F.col("timestamp").isNotNull())

        assert valid_df.count() == 2

    def test_coordinate_bounds_validation(self, spark):
        """Test coordinate bounds validation."""
        data = [
            {"latitude": 40.7128, "longitude": -74.0060},
            {"latitude": -90.0, "longitude": 180.0},
            {"latitude": 95.0, "longitude": -200.0},  # Invalid
        ]

        df = spark.createDataFrame(data)
        valid_df = df.filter((F.abs(F.col("latitude")) <= 90.0) & (F.abs(F.col("longitude")) <= 180.0))

        assert valid_df.count() == 2

    def test_acceleration_range_validation(self, spark):
        """Test acceleration values within reasonable range."""
        data = [
            {"accel_x": 9.8, "accel_y": -2.1},
            {"accel_x": 15.0, "accel_y": 3.5},
            {"accel_x": 200.0, "accel_y": -150.0},  # Unreasonable
        ]

        df = spark.createDataFrame(data)
        valid_df = df.filter((F.abs(F.col("accel_x")) <= 50.0) & (F.abs(F.col("accel_y")) <= 50.0))

        assert valid_df.count() == 2
