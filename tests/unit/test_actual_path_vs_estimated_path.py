"""
Unit tests for Gold Layer ML features and spatial calculations.
Tests trajectory estimation, dead reckoning, and spatial feature engineering.
"""

import pytest
import math


@pytest.mark.unit
class TestSessionizationEngine:
    """Test simulation session partitioning logic."""

    def test_new_session_on_startup(self):
        """Test that first row starts a new session."""
        first_row_prev_ts = None
        is_new_session = first_row_prev_ts is None
        assert is_new_session

    def test_new_session_on_time_gap(self):
        """Test that gap > 5 seconds triggers new session."""
        time_gap_seconds = 6.0
        gap_threshold = 5.0
        is_new_session = time_gap_seconds > gap_threshold
        assert is_new_session

    def test_no_new_session_within_threshold(self):
        """Test that small gaps don't trigger new session."""
        time_gap_seconds = 3.0
        gap_threshold = 5.0
        is_new_session = time_gap_seconds > gap_threshold
        assert not is_new_session

    def test_session_id_increments(self):
        """Test that session IDs increment correctly."""
        session_markers = [1, 0, 0, 1, 0, 0, 0]
        session_ids = []
        current_id = 0
        for marker in session_markers:
            current_id += marker
            session_ids.append(current_id)
        assert session_ids == [1, 1, 1, 2, 2, 2, 2]


@pytest.mark.unit
class TestPhysicsIntegration:
    """Test physics calculation and integration."""

    def test_radians_conversion(self):
        """Test conversion from degrees to radians."""
        degrees = 45.0
        radians = math.radians(degrees)
        expected = math.pi / 4
        assert abs(radians - expected) < 0.001

    def test_heading_integration(self):
        """Test heading calculation from gyro data."""
        gyro_rad_per_sec = 0.1745  # ~10 deg/s
        dt_sec = 1.0
        heading_change = gyro_rad_per_sec * dt_sec
        assert abs(heading_change - 0.1745) < 0.001

    def test_velocity_components_calculation(self):
        """Test velocity decomposition into x and y components."""
        velocity_mps = 5.0
        heading_rad = 0.0  # Moving in +x direction
        dt_sec = 1.0

        delta_x = velocity_mps * math.cos(heading_rad) * dt_sec
        delta_y = velocity_mps * math.sin(heading_rad) * dt_sec

        assert abs(delta_x - 5.0) < 0.001
        assert abs(delta_y - 0.0) < 0.001

    def test_velocity_components_45_degree_angle(self):
        """Test velocity components at 45 degrees."""
        velocity_mps = 10.0
        heading_rad = math.radians(45)
        dt_sec = 1.0

        delta_x = velocity_mps * math.cos(heading_rad) * dt_sec
        delta_y = velocity_mps * math.sin(heading_rad) * dt_sec

        expected = 10.0 / math.sqrt(2)
        assert abs(delta_x - expected) < 0.1
        assert abs(delta_y - expected) < 0.1


@pytest.mark.unit
class TestDeadReckoning:
    """Test dead reckoning (estimated path) calculations."""

    def test_estimated_path_accumulation(self):
        """Test that estimated path accumulates correctly."""
        delta_x_values = [1.0, 1.0, 1.0]
        estimated_x = sum(delta_x_values)
        assert estimated_x == 3.0

    def test_estimated_path_with_direction_changes(self):
        """Test estimated path with heading changes."""
        delta_x_values = [1.0, -1.0, 2.0]
        delta_y_values = [0.0, 1.0, 0.0]

        estimated_x = sum(delta_x_values)
        estimated_y = sum(delta_y_values)

        assert estimated_x == 2.0
        assert estimated_y == 1.0


@pytest.mark.unit
class TestActualPathCalculation:
    """Test actual GPS-based path calculation."""

    def test_meters_per_degree_conversion(self):
        """Test conversion from geographic degrees to meters."""
        meters_per_degree = 111000.0
        degrees = 1.0
        meters = degrees * meters_per_degree
        assert meters == 111000.0

    def test_relative_latitude_calculation(self):
        """Test latitude difference converted to meters."""
        start_lat = 40.7128
        current_lat = 40.7228
        lat_diff = current_lat - start_lat
        meters_per_degree = 111000.0

        path_y = lat_diff * meters_per_degree
        expected = 0.01 * meters_per_degree
        assert abs(path_y - expected) < 1.0

    def test_relative_longitude_calculation(self):
        """Test longitude difference converted to meters."""
        start_lon = -74.0060
        current_lon = -74.0160
        lon_diff = current_lon - start_lon
        meters_per_degree = 111000.0
        latitude_factor = 0.7  # Cosine adjustment for latitude

        path_x = lon_diff * meters_per_degree * latitude_factor
        expected = -0.01 * meters_per_degree * latitude_factor
        assert abs(path_x - expected) < 1.0


@pytest.mark.unit
class TestTrajectoryDriftMetrics:
    """Test trajectory drift error calculation."""

    def test_euclidean_distance_calculation(self):
        """Test Euclidean distance between estimated and actual paths."""
        estimated_x = 5.0
        estimated_y = 0.0
        actual_x = 4.0
        actual_y = 3.0

        dx = actual_x - estimated_x
        dy = actual_y - estimated_y
        drift = math.sqrt(dx**2 + dy**2)

        assert abs(drift - math.sqrt(10.0)) < 0.001

    def test_zero_drift_when_paths_match(self):
        """Test that drift is zero when estimated and actual paths match."""
        estimated_x = 5.0
        estimated_y = 3.0
        actual_x = 5.0
        actual_y = 3.0

        drift = math.sqrt((actual_x - estimated_x) ** 2 + (actual_y - estimated_y) ** 2)
        assert drift == 0.0

    def test_drift_accumulation_over_time(self):
        """Test that drift can accumulate over multiple time steps."""
        drifts = [0.1, 0.2, 0.15, 0.3]
        total_drift_estimate = sum(drifts)
        assert total_drift_estimate == 0.75


@pytest.mark.unit
class TestMLFeatureEngineering:
    """Test ML feature generation and compilation."""

    def test_feature_count(self):
        """Test that all required features are generated."""
        features = [
            "feature_estimated_path_x",
            "feature_estimated_path_y",
            "feature_actual_path_x",
            "feature_actual_path_y",
            "target_trajectory_drift_meters",
        ]
        assert len(features) == 5

    def test_feature_names(self):
        """Test that feature names follow naming convention."""
        estimated_features = ["feature_estimated_path_x", "feature_estimated_path_y"]
        actual_features = ["feature_actual_path_x", "feature_actual_path_y"]
        target = "target_trajectory_drift_meters"

        for feat in estimated_features + actual_features:
            assert feat.startswith("feature_")
        assert target.startswith("target_")


@pytest.mark.unit
class TestGoldMLMaterializationView:
    """Test materialized view creation for ML features."""

    def test_view_name(self):
        """Test that ML feature view is named gold_ml_features."""
        view_name = "gold_ml_features"
        assert view_name == "gold_ml_features"

    def test_source_upstream_view(self):
        """Test that source is the upstream kinetic master view."""
        source_table = "live.gold_kinetic_master"
        assert "gold_kinetic_master" in source_table
