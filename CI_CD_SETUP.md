# CI/CD Pipeline Documentation

## Overview

This project uses GitHub Actions for continuous integration and continuous deployment (CI/CD). The pipeline includes:

1. **CI Workflow** (`ci.yml`) - Runs on push and pull requests to `main` and `develop` branches
2. **CD Workflow** (`cd.yml`) - Deploys to Databricks environments after successful CI
3. **PR Test Workflow** (`test-pr.yml`) - Runs tests on pull requests with detailed reporting

## Test Structure

### Test Organization

Tests are organized in the `tests/` directory:

```
tests/
├── unit/                              # Unit tests (no Spark/Databricks required)
│   ├── test_bronze_layer.py          # Bronze layer logic tests
│   ├── test_silver_imu.py            # Silver layer IMU logic tests
│   ├── test_silver_encoder.py        # Silver layer Encoder logic tests
│   ├── test_silver_gps.py            # Silver layer GPS logic tests
│   ├── test_gold_aggregations.py     # Gold layer metrics logic tests
│   └── test_actual_path_vs_estimated_path.py  # Spatial feature logic tests
├── integration/                       # Integration tests (requires Spark)
│   ├── test_bronze_layer_spark.py    # Bronze layer DataFrame transformations
│   ├── test_silver_layer_spark.py    # Silver layer window & aggregation tests
│   ├── test_gold_layer_spark.py      # Gold layer joins & calculations tests
│   └── README.md                      # Integration test guide
├── conftest.py                        # Pytest configuration and fixtures
└── fixtures/                          # Test data files (JSON, CSV)
```

### Test Markers

Tests are marked with pytest markers for organization:

- `@pytest.mark.unit` - Unit tests that don't require Databricks/Spark
- `@pytest.mark.integration` - Integration tests requiring actual Spark DataFrames
- `@pytest.mark.slow` - Slow-running tests

## Running Tests Locally

### Prerequisites

- Python 3.10, 3.11, or 3.12
- `uv` package manager (recommended) or `pip`

### Setup

#### Using `uv` (Recommended)

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync --all-extras
```

#### Using `pip`

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"
```

### Running Tests

#### Run all unit tests
```bash
uv run pytest tests/unit -v
```

#### Run specific test file
```bash
uv run pytest tests/unit/test_bronze_layer.py -v
```

#### Run specific test class
```bash
uv run pytest tests/unit/test_bronze_layer.py::TestBronzeLayerRawData -v
```

#### Run specific test
```bash
uv run pytest tests/unit/test_bronze_layer.py::TestBronzeLayerRawData::test_bronze_table_definition_exists -v
```

#### Run with coverage report
```bash
uv run pytest tests/unit --cov=src --cov-report=html --cov-report=term-missing
```

#### Run integration tests (Spark-based)
```bash
uv run pytest tests/integration -v
```

#### Run integration tests with coverage
```bash
uv run pytest tests/integration --cov=src --cov-report=html
```

#### Run both unit and integration tests
```bash
uv run pytest tests/ -v
```

## GitHub Actions Workflows

### CI Workflow (ci.yml)

**Triggers:** Push to `main`/`develop`, Pull requests

**Jobs:**

1. **test** - Runs on Python 3.10, 3.11, 3.12
   - Installs dependencies
   - Runs unit tests
   - Uploads test results as artifacts

2. **lint** - Code quality checks with Ruff
   - Checks code style and imports
   - Checks code formatting

3. **code-quality** - Coverage and code metrics
   - Generates coverage reports
   - Uploads to Codecov

### CD Workflow (cd.yml)

**Triggers:** Push to `main` (auto-deploy to dev), Manual workflow dispatch (for staging/prod)

**Jobs:**

1. **deploy-dev** - Deploys to dev environment automatically
   - Uses `databricks bundle deploy --target dev`
   - No approval required

2. **deploy-staging** - Deploys to staging (depends on dev success)
   - Requires staging environment approval
   - Runs integration tests

3. **deploy-prod** - Manual production deployment
   - Triggered via workflow dispatch
   - Requires production environment approval
   - Creates backup and deployment tags

### PR Test Workflow (test-pr.yml)

**Triggers:** Pull requests to `main`/`develop`

**Features:**

- Runs tests on every PR
- Comments PR with test results
- Uploads coverage reports
- Works with Codecov for coverage tracking

## Setting Up Secrets for CD

To enable GitHub Actions deployments, configure the following secrets in your GitHub repository:

### For Dev/Staging Environments

In **Settings > Secrets and variables > Actions**:

```
DATABRICKS_HOST        - Your Databricks workspace URL
DATABRICKS_TOKEN       - Your Databricks PAT token
```

### For Production Environment

```
DATABRICKS_HOST_PROD   - Your production Databricks workspace URL
DATABRICKS_TOKEN_PROD  - Your production Databricks PAT token
```

### Environment Protection

Create GitHub environments for each deployment target:

1. Go to **Settings > Environments**
2. Create environments: `dev`, `staging`, `prod`
3. Set required reviewers for `staging` and `prod`
4. Add environment-specific secrets

## Test Structure

### Unit Tests (tests/unit/)

Fast tests that validate logic without Spark:
- Constants and validation logic
- Configuration values
- Simple calculations
- Data ranges and bounds

**Run:** `uv run pytest tests/unit -v`

**Example:**
```python
@pytest.mark.unit
def test_velocity_conversion():
    ticks_to_meters = 0.001745
    velocity = 1000 * ticks_to_meters
    assert abs(velocity - 1.745) < 0.001
```

### Integration Tests (tests/integration/)

Tests that validate actual Spark DataFrame transformations:
- Filtering and casting (Bronze layer)
- Windowing and aggregations (Silver layer)
- Joins and calculations (Gold layer)
- Spatial feature engineering

**Run:** `uv run pytest tests/integration -v`

**Example:**
```python
@pytest.mark.integration
def test_imu_filtering(self, spark):
    raw_df = spark.createDataFrame([...])
    imu_df = raw_df.filter(F.col("sensor") == "IMU")
    assert imu_df.count() == 2
```

## Test Coverage

### Unit Tests Cover
- Data type conversions
- Numerical calculations
- Validation ranges
- Constants and thresholds
- Configuration values

### Integration Tests Cover
- Spark DataFrame operations
- Filtering and aggregations
- Window functions
- Joins on multiple tables
- Type casting in Spark
- Null value handling

## Troubleshooting

### Tests fail locally but pass in CI

- Ensure you're using the same Python version as CI (3.11 recommended)
- Run `uv sync --all-extras` to install all dependencies
- Clear cache: `uv cache clean`

### Import errors for `pyspark`

- Unit tests don't require PySpark
- Integration tests require Databricks Connect setup
- See conftest.py for Spark fixture configuration

### Deployment fails with authentication error

- Verify Databricks tokens are set correctly in GitHub Secrets
- Check token permissions include workspace and jobs API access
- Ensure tokens haven't expired

### Coverage not uploading to Codecov

- Verify Codecov integration is enabled in your repository
- Check that coverage.xml is being generated
- See Codecov documentation for troubleshooting

## Best Practices

### Writing Tests

1. **One assertion per test** when possible
2. **Descriptive test names** that explain what is being tested
3. **Use fixtures** from conftest.py for shared test data
4. **Mark slow tests** with `@pytest.mark.slow`
5. **Test edge cases** and error conditions

### Committing Changes

1. **Run tests locally** before pushing
2. **Add tests** for new features
3. **Update tests** for modified behavior
4. **Follow commit message** conventions

### Pull Requests

1. **Enable branch protection** requiring CI to pass
2. **Require reviewers** for production branches
3. **Require status checks** before merging
4. **Keep PRs small** for easier review

## Additional Resources

- [Pytest Documentation](https://docs.pytest.org)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Databricks Bundle Documentation](https://docs.databricks.com/en/dev-tools/bundles)
- [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/)

## Maintenance

### Regular Checks

- Update GitHub Actions versions monthly
- Review and update dependencies quarterly
- Monitor Codecov coverage trends
- Archive old test artifacts

### Contributing

When contributing:

1. Ensure all tests pass locally
2. Add tests for new functionality
3. Follow the existing test structure
4. Update documentation as needed
