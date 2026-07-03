# 🌊 KineticLake

**A Cyber-Physical DataOps Platform for Robotic Sensor Fusion & GPS-Denied Localization**

---

## 🛑 The Physical AI Problem & The "Dark Data" Crisis

We are entering the era of **Physical AI** — autonomous systems, warehouse robots, and self-driving vehicles interacting dynamically with the real world. To survive, these robots rely on an array of highly specialized sensors:

- **Localization & Proprioception**: Internal state tracking (IMUs, Wheel Encoders) — *"How am I moving?"*
- **Perception & Exteroception**: Environmental mapping (LiDAR, Radar, Cameras) — *"What is around me?"*
- **Health & Diagnostics**: Telemetry (Thermals, Voltage, Vibration) — *"Am I operating safely?"*

A single autonomous robot can generate **terabytes of telemetry per day**. However, the industry faces a massive **Dark Data Crisis**: up to **90% of this data is used only for real-time edge processing** and is **immediately discarded** due to:

- Massive temporal synchronization issues across sensors running at vastly different frequencies
- Chaotic, evolving data schemas from ROS environments
- No standardized infrastructure bridging edge telemetry and cloud-scale analytics

Without this historical data, ML teams cannot build predictive maintenance models, train reinforcement learning agents, or simulate accurate digital twins.

---

## 🚀 What is KineticLake?

**KineticLake** is a Databricks-powered **Cyber-Physical DataOps Platform** that bridges raw robotic sensor streams and downstream Machine Learning teams.

It is built around a fundamental principle: **the data architecture must mirror the physics of the robot, not the convenience of the data warehouse.**

This means:
- NULL is not missing data — it is a valid physics signal indicating a sensor did not fire at that microsecond
- Aggregation destroys the micro-kinematics ML models need to learn
- Training data must match the resolution at which the model will run in production

KineticLake delivers **two independent pipelines** from a single shared Bronze ingestion layer:

| | Localization ML Pipeline | Fleet Analytics Pipeline |
|---|---|---|
| **Purpose** | Train GPS-denied localization models | Fleet health visibility & operational KPIs |
| **Resolution** | Microsecond — native sensor resolution | 1-second bucketed aggregations |
| **NULL handling** | NULL = valid physics signal | NULL = gap to be filled or dropped |
| **Silver layer** | Event-driven flag structure | SCD2 historical tracking |
| **Gold layer** | Batch Kalman Filter + dual feature stores | Fleet KPIs, terrain classification, smart pruning |
| **Consumer** | ML engineers | Data analysts & fleet operators |

---

## ⚙️ Architecture: Shared Bronze, Dual Pipelines

```
                        ┌─────────────────────────┐
                        │     BRONZE LAYER         │
                        │   (Shared Raw Ingestion) │
                        │                          │
                        │  - Unaltered sensor      │
                        │    streams               │
                        │  - Schema evolution      │
                        │  - Sensor type filtering │
                        │  - Null tracking         │
                        └────────────┬────────────┘
                                     │
                    ┌────────────────┴─────────────────┐
                    │                                   │
        ┌───────────▼──────────┐           ┌───────────▼──────────┐
        │  LOCALIZATION ML     │           │  FLEET ANALYTICS     │
        │  PIPELINE            │           │  PIPELINE            │
        └──────────────────────┘           └──────────────────────┘
                    │                                   │
        ┌───────────▼──────────┐           ┌───────────▼──────────┐
        │  SILVER 1            │           │  SILVER 2            │
        │  Event Stream        │           │  Aggregated          │
        │                      │           │                      │
        │  - Microsecond       │           │  - 1-second windows  │
        │    resolution        │           │  - SCD2 tracking     │
        │  - Execution flags   │           │  - Temporal          │
        │  - NULL preserved    │           │    alignment         │
        │  - Append-only       │           │  - Bounds validation │
        └───────────┬──────────┘           └───────────┬──────────┘
                    │                                   │
        ┌───────────▼──────────┐           ┌───────────▼──────────┐
        │  GOLD 1              │           │  GOLD 2              │
        │  Physics Engine      │           │  Fleet Intelligence  │
        │                      │           │                      │
        │  - Batch KF per      │           │  - Fleet RMSE/MAE    │
        │    drive session     │           │  - Terrain           │
        │  - Feature Store A   │           │    classification    │
        │  - Feature Store B   │           │  - Anomaly flagging  │
        │  - Drift labels      │           │  - Smart pruning     │
        └──────────────────────┘           └──────────────────────┘
```

---

## 🥉 Bronze Layer (Shared)

Single raw ingestion layer serving both downstream pipelines. One source of truth.

- Unaltered ROSbag sensor streams
- Minimal transformation, maximum auditability
- Sensor type filtering via DLT expectations
- Schema evolution handling and null tracking

**Key Tables:**
- `bronze_robotics_raw` — Streaming raw sensor ingestion
- `bronze_imu_data` — Extracted IMU signals
- `bronze_encoder_data` — Extracted wheel encoder ticks
- `bronze_gps_data` — Extracted GPS coordinates

---

## 🔵 Localization ML Pipeline

**Purpose:** Generate training data for GPS-denied localization models — the core ML research objective of KineticLake.

### Silver 1 — Event Stream Layer

The Silver 1 layer preserves the **exact asynchronous reality of the robot's execution loops**. It does not resample, aggregate, or fill gaps.

**Core design principle:** A vehicle traveling at 130 km/h moves 36 meters per second. A 1-second aggregation bucket introduces unacceptable information loss. The micro-vibrations, kinematic spikes, and precise gyro readings that exist *between* GPS pulses are exactly what the ML model must learn from.

**Execution Flag Architecture:**

Each row receives three binary flags indicating which hardware interrupt fired at that microsecond:

| Flag | Value | Meaning |
|------|-------|---------|
| `imu_computed` | 1 | IMU interrupt fired — acceleration and gyro data valid |
| `imu_computed` | 0 | IMU did not fire — `accel_x`, `accel_y`, `gyro_z` are NULL |
| `encoder_computed` | 1 | Encoder interrupt fired — wheel tick data valid |
| `encoder_computed` | 0 | Encoder did not fire — `ticks` is NULL |
| `gps_computed` | 1 | GPS fix received — coordinates valid |
| `gps_computed` | 0 | GPS did not fire — `latitude`, `longitude` are NULL |

**NULL is not an error.** A row where `gps_computed = 0` and `latitude = NULL` is a precise, accurate representation of the robot's sensor state at that microsecond. The Kalman Filter and the ML model both consume these flags directly — enabling the model to learn *"when GPS is absent, how do I estimate position from IMU and encoder alone?"*

**DLT Quality Gates (Silver 1):**
```python
# Timestamp is always required — every event must be anchored in time
@dp.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")

# Flags must be binary — no undefined states
@dp.expect_or_drop("valid_flags",
    "imu_computed IN (0,1) AND encoder_computed IN (0,1) AND gps_computed IN (0,1)")

# At least one sensor must have fired — all-zero rows are pipeline bugs
@dp.expect_or_drop("sensor_fired",
    "NOT (imu_computed = 0 AND encoder_computed = 0 AND gps_computed = 0)")
```

**Key Tables:**
- `silver_event_stream` — Flagged microsecond event log, append-only

---

### Gold 1 — Physics Engine & ML Feature Stores

The Gold 1 layer runs a **batch Kalman Filter over complete drive sessions**. Batch processing is a deliberate architectural choice: computing KF state over a full session provides complete trajectory context per row — impossible in streaming — and eliminates watermark complexity and late arrival uncertainty.

**The ML Objective: Two Feature Stores, One Ground Truth**

KineticLake generates two complementary feature stores for training GPS-denied localization models:

#### Feature Store A — 3-Sensor Fusion (Ground Truth)
- **Sensors**: IMU + Wheel Encoders + GPS
- **Method**: Kalman Filter fusing all three streams
- **GPS role**: Active correction input — periodically snaps the filter to a known position
- **Output**: `feature_corrected_path_x/y` — the best achievable localization estimate
- **Why better than raw GPS**: Continuous, smoothed, higher effective resolution than 1Hz raw satellite signal

#### Feature Store B — GPS-Denied Localization
- **Sensors**: IMU + Wheel Encoders only
- **Method**: Kalman Filter without GPS correction
- **GPS role**: Withheld at runtime — used only as training label via Feature Store A
- **Output**: `feature_estimated_path_x/y` — dead reckoning trajectory
- **Target label**: `target_trajectory_drift_meters` — Euclidean drift between Feature Store B and Feature Store A

> **Design rationale:** Feature Store A's KF-fused trajectory is a higher-quality ground truth than raw GPS. Training Feature Store B against Feature Store A means the ML model learns from the best possible reference — not 1Hz satellite noise.

**Key Metrics:**
- `velocity_mps` — Derived from encoder ticks
- `acceleration_mps2` — Rate of change in velocity
- `heading_theta` — Continuous yaw angle from gyro integration *(global, never reset per session)*
- `kf_state_x/y` — Kalman Filter position estimate per row
- `kf_error_covariance_p` — KF uncertainty matrix — grows during GPS-denied periods
- `feature_estimated_path_x/y` — Dead reckoning trajectory (Feature Store B)
- `feature_corrected_path_x/y` — KF-fused trajectory (Feature Store A / Ground Truth)
- `target_trajectory_drift_meters` — **ML target label**

**Key Tables:**
- `gold_localization_features` — Unified ML feature store with both trajectory sets and target labels

---

## 🟠 Fleet Analytics Pipeline

**Purpose:** Operational fleet visibility, KPI tracking, and intelligent data lifecycle management.

### Silver 2 — Aggregated Layer

The Silver 2 layer is where temporal chaos is resolved for analytical consumption. Three sensors running at completely different frequencies — 100Hz, 10Hz, 1Hz — are resampled to synchronized 1-second windows, making them joinable and analytically meaningful.

- **1-second resampling windows** via DLT watermarks
- **SCD2 historical tracking** via `create_auto_cdc_flow` — full change history preserved
- **Aggregations**: IMU vibration index, encoder-derived velocity, averaged GPS coordinates
- **Bounds and outlier validation** per sensor type

**Key Tables:**
- `silver_imu_history` — Resampled IMU with SCD2 tracking
- `silver_encoder_history` — Resampled encoder with SCD2 tracking
- `silver_gps_history` — Resampled GPS with SCD2 tracking

---

### Gold 2 — Fleet Intelligence

The Gold 2 layer transforms aggregated Silver 2 data into operational insight for fleet operators and data analysts.

**Fleet KPI Tracking:**
- `fleet_rmse_drift` — Root mean squared drift error per robot per session
- `fleet_mae_drift` — Mean absolute error across fleet
- Long-term drift trend analysis for predictive maintenance

**Terrain Classification:**
- 1-second variance windows flagging high-vibration environments
- Automatic classification: normal / pothole / black ice / rough terrain
- Feeds contextual labels back to Gold 1 ML features

**Smart Data Pruning:**
- Automatically identifies zero-drift, low-variance 100Hz raw segments
- Flags boring data for deletion — preserving only high-value anomaly windows
- Reduces Bronze layer storage cost without losing meaningful signal

**Dynamic Quality Rules:**
Quality gates in Gold 2 are loaded from `iot_data.gold.dim_sensor_metadata` at runtime — not hardcoded. When sensor thresholds change, the pipeline adapts without code deployment.

**Key Tables:**
- `gold_fleet_kpis` — Fleet-level drift metrics and health indicators
- `gold_terrain_classification` — Session-level terrain context labels
- `gold_pruning_manifest` — Smart pruning decisions and deletion log

---

## 📊 Integrated Sensors (Phase 1: Localization & Kinematics)

| Sensor | Frequency | Raw Signals | Silver 1 (ML) | Silver 2 (Analytics) |
|--------|-----------|-------------|----------------|----------------------|
| IMU | 100 Hz | `accel_x/y`, `gyro_z` | Flagged events, NULL preserved | Vibration index, avg gyro |
| Wheel Encoder | 10 Hz | `ticks` (cumulative) | Flagged events, NULL preserved | `total_ticks_per_sec` → velocity |
| GPS | 1 Hz | `latitude`, `longitude` | Flagged events, NULL preserved | Averaged coordinates |

---

## 🔮 Future Roadmap

### Phase 2: Kalman Filter Implementation
- Implement batch KF as **Pandas UDF** (`applyInPandas`) grouped by `session_id`
- Feature Store A: Extended KF fusing IMU + encoder + GPS
- Feature Store B: IMU + encoder only KF for GPS-denied estimation
- `robot_id` promoted as first-class partition key for multi-robot fleet scaling

### Phase 3: ML Model Training
- LSTM or Transformer sequence model on Silver 1 flagged event matrices
- Target: predict `target_trajectory_drift_meters` from Feature Store B features
- Inference output feeds back into the KF as a drift correction signal

### Phase 4: LLM-Driven Schema Adaptation
- Automated parsing of new ROSbag manifests via LLM
- Dynamic generation of Bronze/Silver DLT expectations for new sensor types
- Auto-generated data dictionaries

### Phase 5: Unstructured Perception Data
- **LiDAR**: 3D Point Clouds (Apache Sedona)
- **Radar**: Doppler velocity mapping
- **Cameras**: 2D Visual feeds (Databricks Mosaic)

---

## 📁 Project Structure

```
KineticLake/
├── src/
│   ├── bronze/
│   │   └── transformations/
│   │       └── Bronze_Layer.py              # Shared raw ingestion
│   ├── silver/
│   │   └── transformations/
│   │       ├── silver_event_stream.py       # Silver 1 — ML pipeline
│   │       ├── silver_imu.py                # Silver 2 — Analytics pipeline
│   │       ├── silver_encoder.py            # Silver 2 — Analytics pipeline
│   │       └── silver_gps.py                # Silver 2 — Analytics pipeline
│   └── gold/
│       └── transformations/
│           ├── gold_localization_features.py  # Gold 1 — KF + ML feature stores
│           ├── gold_fleet_kpis.py             # Gold 2 — Fleet KPIs
│           ├── gold_terrain_classification.py # Gold 2 — Terrain context
│           └── gold_pruning_manifest.py       # Gold 2 — Smart pruning
├── resources/
│   ├── bronze_layer.pipeline.yml
│   ├── silver_ml.pipeline.yml               # ML pipeline
│   ├── silver_analytics.pipeline.yml        # Analytics pipeline
│   ├── gold_ml.pipeline.yml
│   ├── gold_analytics.pipeline.yml
│   └── whole_workflow.job.yml
├── tests/
│   ├── unit/
│   └── integration/
├── .github/workflows/
│   ├── ci.yml
│   ├── cd.yml
│   └── test-pr.yml
├── databricks.yml
├── pyproject.toml
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- **Python** 3.10+ (3.11 recommended)
- **uv** package manager: https://astral.sh/uv
- **Databricks** workspace (for production deployment)
- **Git**

### Local Setup

```bash
# Clone and navigate
git clone <your-repo> && cd KineticLake

# Install dependencies
uv sync --all-extras

# Run unit tests
uv run pytest tests/unit -v

# Lint check
uv run ruff check src/ tests/
```

---

## 🧪 Running Tests

```bash
# Unit tests — fast, no Spark
uv run pytest tests/unit -v

# Integration tests — Spark-based
uv run pytest tests/integration -v

# Full suite with coverage
uv run pytest tests/ --cov=src --cov-report=term-missing
```

---

## 🚀 Deployment

```bash
# Authenticate
databricks configure

# Dev — auto on push to main
databricks bundle deploy --target dev

# Staging — requires 1 reviewer approval
databricks bundle deploy --target staging

# Production — requires 2 reviewer approvals
databricks bundle deploy --target prod
```

**Required GitHub Secrets:**
```
DATABRICKS_HOST    # Workspace URL
DATABRICKS_TOKEN   # Personal Access Token
```

---

## 💻 Built With

- **Apache Spark / PySpark** — Distributed processing
- **Databricks Delta Live Tables (DLT)** — Streaming pipelines & data quality
- **Databricks Asset Bundles (DABs)** — Infrastructure as Code
- **GitHub Actions & uv** — CI/CD & deterministic environments
- **Pytest** — Unit and integration testing

---

## 🤝 Contributing

1. Fork and clone the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Write tests for your changes
4. Run: `uv run pytest tests/ -v`
5. Format: `uv run ruff format src/ tests/`
6. Open a Pull Request — CI/CD runs automatically

---

## 📄 License

Apache 2.0

---

**Built for the Physical AI community — maintained by [Hjadall](https://github.com/Hjadall)**
