"""
Integration tests for Bronze Layer transformations.
Tests actual Spark DataFrame transformations for raw data ingestion.
"""

import pytest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType


@pytest.mark.integration
class TestBronzeRawDataIngestion:
    """Test Bronze layer raw data ingestion with actual Spark transformations."""

    @pytest.fixture
    def raw_sensor_data(self, spark):
        """Create sample raw sensor data."""
        data = [
            {
                "sensor": "IMU",
                "data": {"accel_x": 9.8, "accel_y": -2.1, "gyro_z": 0.05},
                "ts": datetime(2026, 5, 10, 14, 32, 27),
            },
            {
                "sensor": "IMU",
                "data": {"accel_x": 9.9, "accel_y": -2.2, "gyro_z": 0.06},
                "ts": datetime(2026, 5, 10, 14, 32, 28),
            },
            {"sensor": "ENCODER", "data": {"ticks": 1250}, "ts": datetime(2026, 5, 10, 14, 32, 27)},
            {"sensor": "GPS", "data": {"lat": 40.7128, "lon": -74.0060}, "ts": datetime(2026, 5, 10, 14, 32, 27)},
            {"sensor": "GPS", "data": {"lat": 40.7138, "lon": -74.0050}, "ts": datetime(2026, 5, 10, 14, 32, 28)},
        ]
        return spark.createDataFrame(data)

    def test_bronze_robotics_raw_ingestion(self, spark, raw_sensor_data):
        """Test that raw sensor data is ingested correctly."""
        df = raw_sensor_data
        assert df.count() == 5
        assert "sensor" in df.columns
        assert "data" in df.columns
        assert "ts" in df.columns

    def test_filter_imu_sensor_data(self, spark, raw_sensor_data):
        """Test filtering IMU sensor data."""
        imu_df = raw_sensor_data.filter(F.col("sensor") == "IMU").select(
            F.col("sensor").cast("string").alias("sensor"),
            F.col("data.accel_x").cast("double").alias("accel_x"),
            F.col("data.accel_y").cast("double").alias("accel_y"),
            F.col("data.gyro_z").cast("double").alias("gyro_z"),
            F.col("ts").cast("timestamp").alias("timestamp"),
        )

        assert imu_df.count() == 2
        assert set(imu_df.columns) == {"sensor", "accel_x", "accel_y", "gyro_z", "timestamp"}

        rows = imu_df.collect()
        assert rows[0]["sensor"] == "IMU"
        assert rows[0]["accel_x"] == 9.8
        assert rows[0]["accel_y"] == -2.1
        assert rows[0]["gyro_z"] == 0.05

    def test_filter_encoder_sensor_data(self, spark, raw_sensor_data):
        """Test filtering Encoder sensor data."""
        encoder_df = raw_sensor_data.filter(F.col("sensor") == "ENCODER").select(
            F.col("sensor").cast("string").alias("sensor"),
            F.col("data.ticks").cast("long").alias("ticks"),
            F.col("ts").cast("timestamp").alias("timestamp"),
        )

        assert encoder_df.count() == 1
        assert set(encoder_df.columns) == {"sensor", "ticks", "timestamp"}

        row = encoder_df.collect()[0]
        assert row["sensor"] == "ENCODER"
        assert row["ticks"] == 1250

    def test_filter_gps_sensor_data(self, spark, raw_sensor_data):
        """Test filtering GPS sensor data."""
        gps_df = raw_sensor_data.filter(F.col("sensor") == "GPS").select(
            F.col("sensor").cast("string").alias("sensor"),
            F.col("data.lat").cast("double").alias("latitude"),
            F.col("data.lon").cast("double").alias("longitude"),
            F.col("ts").cast("timestamp").alias("timestamp"),
        )

        assert gps_df.count() == 2
        assert set(gps_df.columns) == {"sensor", "latitude", "longitude", "timestamp"}

        rows = gps_df.collect()
        assert rows[0]["latitude"] == 40.7128
        assert rows[0]["longitude"] == -74.0060

    def test_data_type_casting(self, spark, raw_sensor_data):
        """Test that all data types are cast correctly."""
        imu_df = raw_sensor_data.filter(F.col("sensor") == "IMU").select(
            F.col("sensor").cast("string").alias("sensor"),
            F.col("data.accel_x").cast("double").alias("accel_x"),
            F.col("data.accel_y").cast("double").alias("accel_y"),
            F.col("data.gyro_z").cast("double").alias("gyro_z"),
            F.col("ts").cast("timestamp").alias("timestamp"),
        )

        schema = imu_df.schema
        type_dict = {field.name: str(field.dataType) for field in schema.fields}

        assert type_dict["sensor"] == "StringType()"
        assert type_dict["accel_x"] == "DoubleType()"
        assert type_dict["accel_y"] == "DoubleType()"
        assert type_dict["gyro_z"] == "DoubleType()"
        assert type_dict["timestamp"] == "TimestampType()"

    def test_multiple_sensor_types_simultaneously(self, spark, raw_sensor_data):
        """Test that all sensor types can be extracted from same raw data."""
        imu_count = raw_sensor_data.filter(F.col("sensor") == "IMU").count()
        encoder_count = raw_sensor_data.filter(F.col("sensor") == "ENCODER").count()
        gps_count = raw_sensor_data.filter(F.col("sensor") == "GPS").count()

        total = imu_count + encoder_count + gps_count
        assert total == raw_sensor_data.count()
        assert imu_count == 2
        assert encoder_count == 1
        assert gps_count == 2

    def test_timestamp_preservation(self, spark, raw_sensor_data):
        """Test that timestamps are preserved during filtering and casting."""
        imu_df = raw_sensor_data.filter(F.col("sensor") == "IMU").select(
            F.col("ts").cast("timestamp").alias("timestamp")
        )

        timestamps = [row["timestamp"] for row in imu_df.collect()]
        assert len(timestamps) == 2
        assert all(isinstance(ts, datetime) for ts in timestamps)

    def test_null_handling_in_nested_fields(self, spark):
        """Test handling of null values in nested data fields."""
        data_with_nulls = [
            {
                "sensor": "IMU",
                "data": {"accel_x": 9.8, "accel_y": None, "gyro_z": 0.05},
                "ts": datetime(2026, 5, 10, 14, 32, 27),
            },
        ]
        df = spark.createDataFrame(data_with_nulls)

        imu_df = df.filter(F.col("sensor") == "IMU").select(
            F.col("data.accel_x").cast("double").alias("accel_x"),
            F.col("data.accel_y").cast("double").alias("accel_y"),
        )

        row = imu_df.collect()[0]
        assert row["accel_x"] == 9.8
        assert row["accel_y"] is None

    def test_large_batch_data(self, spark):
        """Test processing larger batch of sensor data."""
        data = []
        for i in range(1000):
            sensor_type = ["IMU", "ENCODER", "GPS"][i % 3]
            if sensor_type == "IMU":
                data.append(
                    {
                        "sensor": sensor_type,
                        "data": {"accel_x": 9.8 + i * 0.001, "accel_y": -2.1, "gyro_z": 0.05},
                        "ts": datetime(2026, 5, 10, 14, 32, 27 + i),
                    }
                )
            elif sensor_type == "ENCODER":
                data.append(
                    {"sensor": sensor_type, "data": {"ticks": 1000 + i}, "ts": datetime(2026, 5, 10, 14, 32, 27 + i)}
                )
            else:
                data.append(
                    {
                        "sensor": sensor_type,
                        "data": {"lat": 40.7128 + i * 0.0001, "lon": -74.0060 + i * 0.0001},
                        "ts": datetime(2026, 5, 10, 14, 32, 27 + i),
                    }
                )

        df = spark.createDataFrame(data)

        imu_count = df.filter(F.col("sensor") == "IMU").count()
        encoder_count = df.filter(F.col("sensor") == "ENCODER").count()
        gps_count = df.filter(F.col("sensor") == "GPS").count()

        assert imu_count == 334  # 1000 / 3, rounded up for IMU
        assert encoder_count == 333
        assert gps_count == 333
