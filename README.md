# 🌊 KineticLake

**An Enterprise Lakehouse Architecture for Robotic Sensor Fusion & Physical AI**

---

## 🛑 The Physical AI Problem & The "Dark Data" Crisis

We are entering the era of **Physical AI**—autonomous systems, warehouse robots, and self-driving vehicles interacting dynamically with the real world. To survive, these robots rely on an array of highly specialized sensors, which broadly fall into three categories:

- **Localization & Proprioception**: Internal state tracking (IMUs, Wheel Encoders) to answer *"How am I moving?"*
- **Perception & Exteroception**: Environmental mapping (LiDAR, Radar, Cameras) to answer *"What is around me?"*
- **Health & Diagnostics**: Telemetry (Thermals, Voltage, Vibration) to answer *"Am I operating safely?"*

A single autonomous robot can generate **terabytes of telemetry per day**. However, the industry faces a massive **Dark Data Crisis**: up to **90% of this data is used for real-time edge processing** (to keep the robot from crashing) and is **immediately discarded**. It is rarely piped back into offline Machine Learning data stores due to:

- Massive temporal synchronization issues
- Varying frequencies (e.g., 100Hz IMU vs. 1Hz GPS)
- Chaotic, evolving data schemas

Without this historical data, ML teams cannot build predictive maintenance models, train reinforcement learning algorithms, or simulate accurate digital twins.

---

## 🚀 The Solution: What is KineticLake?

**KineticLake** is a Databricks-powered Data Engineering Lakehouse built specifically to solve the robotic "dark data" problem.

It acts as the **bridge between raw robotic operating system logs (ROS) and downstream Machine Learning teams**. By ingesting raw sensor streams, KineticLake automatically:

✅ Cleans and validates data  
✅ Temporally aligns disjointed telematics  
✅ Mathematically fuses sensor streams  
✅ Generates ML-ready feature stores  

Instead of Data Scientists wasting 80% of their time aligning timestamps and calculating physics vectors, KineticLake delivers a pristine, mathematically sound **"Platinum" dataset** where features and target labels (like sensor drift) are already engineered.

---

## ⚙️ Architecture: Medallion Pattern (Bronze → Silver → Gold)

KineticLake follows the **Medallion Architecture** for enterprise data governance:

### 🥉 Bronze Layer (Raw Data Ingestion)
- Ingests raw sensor streams from ROSbags
- Minimal transformation, maximum auditability
- Filters by sensor type (IMU, ENCODER, GPS)
- Data Quality: Schema evolution handling, null value tracking

**Key Tables:**
- `bronze_robotics_raw` - Streaming raw sensor ingestion
- `bronze_imu_data` - Extracted IMU signals
- `bronze_encoder_data` - Extracted wheel encoder ticks
- `bronze_gps_data` - Extracted GPS coordinates

### 🥈 Silver Layer (Cleaned & Standardized)
- **Resamples** disjointed sensor streams to 1-second windows
- **Aggregates** multiple readings into meaningful metrics
- **SCD2 Tracking** - Maintains history of all changes
- Data Quality: Bounds checking, outlier removal, temporal validation

**Key Transformations:**
- IMU: `avg_accel_x`, `vibration_index_x`, `avg_gyro_z`
- Encoder: `total_ticks_per_sec` → velocity (m/s)
- GPS: Averaged lat/lon coordinates

**Key Tables:**
- `silver_imu_history` - Resampled IMU with SCD2 tracking
- `silver_encoder_history` - Resampled encoder with SCD2 tracking
- `silver_gps_history` - Resampled GPS with SCD2 tracking

### 🥇 Gold Layer (ML-Ready Features & Physics)
- **Joins** synchronized silver streams
- **Calculates** physics-based metrics (acceleration, velocity, heading)
- **Dead Reckoning Engine**: Integrates IMU yaw rates + encoder velocity over time
- **Sensor Fusion**: Maps estimated path vs. GPS ground truth
- **Computes target labels**: Trajectory drift error for predictive models

**Key Metrics:**
- `velocity_mps` - Derived from encoder ticks
- `acceleration_mps2` - Rate of change in velocity
- `heading_theta` - Yaw angle from gyro integration
- `estimated_path_x/y` - Dead reckoning trajectory
- `actual_path_x/y` - GPS-based ground truth
- `trajectory_drift_meters` - **Target label for ML** (sensor degradation predictor)

**Key Tables:**
- `gold_kinetic_master` - Synchronized, quality-checked master dataset
- `gold_ml_features` - ML-ready feature set with target labels

---

## 📊 Integrated Sensors (Phase 1: Localization & Kinematics)

### IMU (Inertial Measurement Unit)
- **Frequency**: 100 Hz
- **Raw Signals**: 
  - `accel_x, accel_y` (linear acceleration in m/s²)
  - `gyro_z` (angular velocity in degrees/second)
- **Silver Aggregations**: Average, standard deviation (vibration index), min/max spread
- **Gold Calculations**: Used for heading integration and acceleration derivation

### Wheel Encoders
- **Frequency**: 10 Hz (per rotation)
- **Raw Signal**: `ticks` (cumulative rotational counts)
- **Conversion**: `0.001745 m/tick` → velocity in m/s
- **Silver Aggregations**: `total_ticks_per_sec`
- **Gold Calculations**: Velocity and acceleration derivatives

### GPS (Global Positioning System)
- **Frequency**: 1 Hz
- **Raw Signals**: `latitude, longitude` (WGS84 coordinates)
- **Silver Aggregations**: Averaged coordinates (noise reduction)
- **Gold Calculations**: Ground truth path mapping, drift error computation

---

## 🔮 Future Roadmap & Expansion

### Phase 2: LLM-Driven Metadata Integration
Every time a new sensor is mounted, the data schema changes. We plan to integrate **Large Language Models (LLMs)** directly into the ingestion pipeline:
- Parse new ROSbag manifests automatically
- Identify sensor types via LLM
- Dynamically generate Bronze/Silver DLT expectations
- Auto-generate data dictionaries

### Phase 3: Tackling Unstructured Perception Data
Expand beyond tabular time-series to ingest:
- **LiDAR**: 3D Point Clouds
- **Radar**: Doppler velocity mapping
- **Cameras**: 2D Visual feeds

This requires integrating advanced computer vision pipelines and distributed array processing (Apache Sedona, Databricks Mosaic).

---

## 📁 Project Structure

```
KineticLake/
├── src/                                      # Python transformation code
│   ├── bronze/
│   │   └── transformations/
│   │       └── Bronze_Layer.py              # Raw data ingestion & filtering
│   ├── silver/
│   │   └── transformations/
│   │       ├── silver_imu.py                # IMU resampling & SCD2
│   │       ├── silver_encoder.py            # Encoder resampling & SCD2
│   │       └── silver_gps.py                # GPS resampling & SCD2
│   └── gold/
│       └── transformations/
│           ├── gold_aggregations.py         # Kinetic master metrics
│           └── actual_path_vs_estimated_path.py  # ML features & drift
├── resources/                                # Databricks Bundle configurations
│   ├── bronze_layer.pipeline.yml            # Bronze DLT pipeline
│   ├── silver_layer.pipeline.yml            # Silver DLT pipeline
│   ├── gold_layer.pipeline.yml              # Gold DLT pipeline
│   └── whole_workflow.job.yml               # Orchestrated workflow
├── tests/                                    # Test suites
│   ├── unit/                                # Unit tests (no Spark required)
│   │   ├── test_bronze_layer.py
│   │   ├── test_silver_imu.py
│   │   ├── test_silver_encoder.py
│   │   ├── test_silver_gps.py
│   │   ├── test_gold_aggregations.py
│   │   └── test_actual_path_vs_estimated_path.py
│   ├── integration/                         # Integration tests (Spark-based)
│   │   ├── test_bronze_layer_spark.py       # DataFrame transformations
│   │   ├── test_silver_layer_spark.py       # Window & aggregation tests
│   │   ├── test_gold_layer_spark.py         # Join & calculation tests
│   │   └── README.md                        # Integration test guide
│   ├── conftest.py                          # Pytest fixtures (Spark session)
│   └── fixtures/                            # Test data (JSON, CSV)
├── .github/workflows/                        # GitHub Actions CI/CD
│   ├── ci.yml                               # Test, lint, coverage
│   ├── cd.yml                               # Deploy to Databricks
│   └── test-pr.yml                          # PR-specific testing
├── databricks.yml                            # Databricks Bundle config
├── pyproject.toml                            # Python dependencies & pytest config
├── CI_CD_SETUP.md                            # Comprehensive CI/CD guide
├── QUICK_START.md                            # Quick reference guide
└── README.md                                 # This file
```

---

## 🚀 Getting Started

### Prerequisites

- **Python** 3.10+ (3.11 recommended)
- **uv** package manager: https://astral.sh/uv/getting-started/installation/
- **Databricks** workspace (for production deployment)
- **Git** (for version control)

### Local Setup (5 minutes)

#### 1. Clone and Navigate
```bash
git clone <your-repo>
cd KineticLake
```

#### 2. Install Dependencies
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync --all-extras
```

#### 3. Verify Installation
```bash
# Run quick tests
uv run pytest tests/unit -v --tb=short

# Check Databricks connectivity (if using Databricks Connect)
uv run databricks configure
```

---

## 📝 How to Use the Repository

### Running Tests Locally

#### Unit Tests (Fast, no Spark required)
```bash
# Run all unit tests
uv run pytest tests/unit -v

# Run specific test file
uv run pytest tests/unit/test_bronze_layer.py -v

# Run with coverage report
uv run pytest tests/unit --cov=src --cov-report=html --cov-report=term-missing
```

#### Integration Tests (Spark-based, actual transformations)
```bash
# Run all integration tests
uv run pytest tests/integration -v

# Run specific integration test
uv run pytest tests/integration/test_bronze_layer_spark.py -v

# Run both unit + integration
uv run pytest tests/ -v
```

### Code Quality Checks

```bash
# Lint with Ruff
uv run ruff check src/ tests/

# Format code
uv run ruff format src/ tests/

# Run all checks
uv run ruff check src/ tests/ && uv run ruff format src/ tests/ --check
```

---

## 🔧 Development Workflow

### Making Code Changes

```bash
# 1. Create a feature branch
git checkout -b feature/my-feature

# 2. Make changes to transformation code
# Edit: src/bronze/transformations/Bronze_Layer.py (example)

# 3. Write tests for your changes
# Create: tests/unit/test_my_feature.py

# 4. Run tests locally
uv run pytest tests/ -v

# 5. Format and lint
uv run ruff format src/ tests/
uv run ruff check src/ tests/

# 6. Commit and push
git add .
git commit -m "Add my feature with tests"
git push origin feature/my-feature

# 7. Create Pull Request on GitHub
# GitHub Actions will automatically run tests
```

### Adding New Transformations

#### Bronze Layer Example
```python
# src/bronze/transformations/Bronze_Layer.py
@dp.table(
    name="bronze_new_sensor",
    comment="Raw new sensor data"
)
def bronze_new_sensor():
    return (
        spark.readStream.table("bronze_robotics_raw")
        .filter(F.col("sensor") == "NEW_SENSOR")
        .select(...)
    )

# Add corresponding test: tests/unit/test_new_sensor.py
```

---

## 🚀 Deployment to Databricks

### Using Databricks Bundles (Infrastructure as Code)

#### Prerequisites
```bash
# Authenticate to Databricks
databricks configure

# Enter your workspace URL and PAT token when prompted
```

#### Deploy to Dev Environment
```bash
# Automatic deployment on push to main branch (via GitHub Actions)
# Or manually deploy:
databricks bundle deploy --target dev
```

#### Deploy to Staging/Production
```bash
# Manual trigger via GitHub Actions (requires approval)
# Or manually:
databricks bundle deploy --target staging
databricks bundle deploy --target prod
```

#### Run Pipelines/Jobs
```bash
# List available jobs
databricks bundle run

# Run specific job
databricks bundle run <job_name>
```

---

## 📊 CI/CD Pipeline (GitHub Actions)

Automatically runs on every push and pull request:

### 🧪 CI Workflow (`.github/workflows/ci.yml`)
- **Trigger**: Push to `main`/`develop`, all PRs
- **Tests**: Python 3.10, 3.11, 3.12
- **Checks**: Unit tests, integration tests, linting, coverage
- **Artifacts**: Test results, coverage reports

### 🚀 CD Workflow (`.github/workflows/cd.yml`)
- **Auto-deploy**: `main` → `dev` (automatic, no approval)
- **Manual deploy**: `staging` (requires reviewer approval)
- **Manual deploy**: `prod` (requires 2 reviewers)

### 📋 PR Test Workflow (`.github/workflows/test-pr.yml`)
- **Trigger**: All PRs to `main`/`develop`
- **Features**: Full test suite, coverage, Codecov integration
- **PR Comments**: Auto-comment with test results

**To enable CD, add GitHub Secrets:**
1. Go to repository **Settings → Secrets and variables → Actions**
2. Add:
   ```
   DATABRICKS_HOST      (your workspace URL)
   DATABRICKS_TOKEN     (your PAT token)
   ```

For more details, see [CI_CD_SETUP.md](CI_CD_SETUP.md) or [QUICK_START.md](QUICK_START.md).

---

## 📚 Working with Data

### Accessing Tables in Databricks

```python
# In notebooks or jobs
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read Bronze layer
bronze_raw = spark.read.table("iot_data.bronze.bronze_robotics_raw")

# Read Silver layer (only current version)
silver_imu = (
    spark.read.table("iot_data.silver.silver_imu_history")
    .filter("__END_AT IS NULL")  # SCD2: current version only
)

# Read Gold layer
gold_features = spark.read.table("iot_data.gold.gold_ml_features")
```

### Understanding Data Quality

Each layer has built-in expectations:

```python
# Bronze: Schema evolution, type safety
@dp.expect_or_drop("valid_sensor_type", "sensor IN ('IMU', 'ENCODER', 'GPS')")

# Silver: Range validation, null checks
@dp.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")

# Gold: Physics constraints, safety limits
@dp.expect_all_or_drop({
    "vibration_safety": "ABS(avg_accel_x) <= 15.0",
    "velocity_safety": "velocity_mps <= 45.0"
})
```

---

## 🐛 Troubleshooting

### **Issue: Tests fail with "ModuleNotFoundError"**
```bash
# Solution: Reinstall dependencies
uv sync --all-extras
```

### **Issue: Databricks deployment fails with auth error**
```bash
# Solution: Verify credentials
databricks configure --token  # Re-authenticate
databricks workspace get-status /  # Test connection
```

### **Issue: Spark integration tests won't run**
```bash
# Solution: Ensure PySpark is installed
uv sync --all-extras

# For Databricks Connect (production):
uv pip install databricks-connect>=15.4,<15.5
```

### **Issue: GitHub Actions deployment stuck**
- Check GitHub Actions logs: **Actions** tab → workflow run → job logs
- Verify secrets are configured correctly
- Ensure workflow has permission to write tokens

For more troubleshooting, see [CI_CD_SETUP.md](CI_CD_SETUP.md).

---

## 📖 Documentation

- **[CI_CD_SETUP.md](CI_CD_SETUP.md)** - Comprehensive CI/CD configuration guide
- **[QUICK_START.md](QUICK_START.md)** - Quick reference for common tasks
- **[tests/integration/README.md](tests/integration/README.md)** - Integration test guide
- **[.github/workflows/README.md](.github/workflows/README.md)** - Workflow reference

---

## 💻 Built With

- **Apache Spark / PySpark** - Core distributed processing framework
- **Databricks Delta Live Tables (DLT)** - Streaming architecture & Data Quality expectations
- **Databricks Asset Bundles (DABs)** - Infrastructure-as-Code (IaC) deployment
- **GitHub Actions & uv** - Lightning-fast CI/CD & deterministic test environments
- **Pytest** - Comprehensive testing framework with fixtures

---

## 🤝 Contributing

1. **Fork and clone** the repository
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Write tests** for your changes
4. **Ensure all tests pass**: `uv run pytest tests/ -v`
5. **Format code**: `uv run ruff format src/ tests/`
6. **Commit changes**: `git commit -m "Add amazing feature"`
7. **Push to branch**: `git push origin feature/amazing-feature`
8. **Open a Pull Request** - CI/CD runs automatically

---

## 📞 Support

For issues, questions, or suggestions:
- **GitHub Issues**: File a bug report or feature request
- **Documentation**: Check [CI_CD_SETUP.md](CI_CD_SETUP.md) and [QUICK_START.md](QUICK_START.md)
- **Databricks Docs**: https://docs.databricks.com

---

## 📄 License

[Add your license here - e.g., MIT, Apache 2.0, etc.]

---

**Built with ❤️ for the Physical AI community**
