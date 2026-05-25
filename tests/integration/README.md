# Integration Tests for KineticLake

This directory contains integration tests that test the actual Spark DataFrame transformations using mocked Spark DataFrames.

## Overview

Integration tests validate the core data transformation logic:

- **Bronze Layer** - Raw data ingestion and filtering by sensor type
- **Silver Layer** - Resampling, windowing, and SCD2 tracking
- **Gold Layer** - Metrics calculation, joins, and spatial feature engineering

## Test Files

### test_bronze_layer_spark.py
Tests Bronze layer transformations:
- Raw data ingestion from nested JSON structures
- Filtering by sensor type (IMU, ENCODER, GPS)
- Data type casting (double, long, timestamp)
- Nested field extraction
- Null value handling

### test_silver_layer_spark.py
Tests Silver layer transformations:
- 1-second windowing and aggregations
- Acceleration metrics (average, spread, standard deviation)
- Encoder ticks aggregation and velocity calculation
- GPS coordinate averaging and stability
- Streaming watermark logic
- SCD2 column tracking (current version filtering)
- Data quality validation (bounds checking)

### test_gold_layer_spark.py
Tests Gold layer transformations:
- Joining multiple Silver tables on timestamp
- Velocity calculation from encoder ticks
- Acceleration calculation from velocity changes
- Data quality checks (spatial bounds, thresholds)
- Sessionization engine (time gap detection)
- Heading calculation from gyro data
- Velocity decomposition into x/y components
- Estimated path accumulation (dead reckoning)
- Actual path calculation from GPS
- Trajectory drift calculation (sensor fusion error)
- ML feature compilation

## Running Integration Tests

### Prerequisites

Ensure Spark and Databricks Connect are available:

```bash
uv sync --all-extras
```

### Run all integration tests

```bash
uv run pytest tests/integration -v
```

### Run specific test file

```bash
uv run pytest tests/integration/test_bronze_layer_spark.py -v
```

### Run specific test class

```bash
uv run pytest tests/integration/test_bronze_layer_spark.py::TestBronzeRawDataIngestion -v
```

### Run specific test

```bash
uv run pytest tests/integration/test_bronze_layer_spark.py::TestBronzeRawDataIngestion::test_bronze_robotics_raw_ingestion -v
```

### Run with coverage

```bash
uv run pytest tests/integration --cov=src --cov-report=html --cov-report=term-missing
```

### Run only integration tests (exclude unit)

```bash
uv run pytest tests/integration -v -m integration
```

### Run unit and integration tests together

```bash
uv run pytest tests/ -v
```

## Test Structure

Each test file follows this structure:

```python
@pytest.mark.integration
class TestSomeComponent:
    """Test description."""
    
    @pytest.fixture
    def sample_data(self, spark):
        """Create test data using Spark fixture."""
        # Create DataFrames with test data
        return spark.createDataFrame(...)
    
    def test_some_transformation(self, spark, sample_data):
        """Test specific transformation logic."""
        # Apply transformation
        result = sample_data.filter(...).groupBy(...).agg(...)
        
        # Assert results
        assert result.count() == expected_count
```

### Using the Spark Fixture

The `spark` fixture from `conftest.py` provides a `DatabricksSession` or local Spark session:

```python
def test_example(self, spark):
    # Create DataFrames
    df = spark.createDataFrame([
        {"col1": 1, "col2": "a"},
        {"col1": 2, "col2": "b"}
    ])
    
    # Apply transformations
    result = df.filter(df.col1 > 1)
    
    # Verify results
    assert result.count() == 1
```

## Mocking Strategy

Integration tests use **actual Spark DataFrames** instead of mocking:

### Why not mock?

1. **Validate actual Spark behavior** - Test real DataFrame operations
2. **Catch Spark-specific issues** - Type casting, null handling, windowing
3. **Easy maintenance** - Use actual Spark API
4. **Better coverage** - Test real data transformations

### DataFrame Creation

Create test data with `spark.createDataFrame()`:

```python
# Simple data
data = [{"col1": 1, "col2": "a"}]
df = spark.createDataFrame(data)

# With schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("col1", IntegerType()),
    StructField("col2", StringType())
])
df = spark.createDataFrame(data, schema)
```

## Common Patterns

### Testing filters

```python
def test_filter_by_condition(self, spark):
    data = [{"status": "active"}, {"status": "inactive"}]
    df = spark.createDataFrame(data)
    
    result = df.filter(F.col("status") == "active")
    assert result.count() == 1
```

### Testing aggregations

```python
def test_aggregation(self, spark):
    data = [
        {"group": "A", "value": 10},
        {"group": "A", "value": 20},
        {"group": "B", "value": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("group").agg(F.sum("value").alias("total"))
    assert result.count() == 2
```

### Testing window functions

```python
def test_window_function(self, spark):
    data = [
        {"timestamp": datetime(...), "value": 1},
        {"timestamp": datetime(...), "value": 2}
    ]
    df = spark.createDataFrame(data)
    
    window = Window.orderBy("timestamp")
    result = df.withColumn("cumsum", F.sum("value").over(window))
```

### Testing joins

```python
def test_join(self, spark, df1, df2):
    result = df1.join(df2, on="key", how="left")
    assert "column_from_df2" in result.columns
```

## Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'pyspark'"

```bash
# Solution: Install PySpark
uv sync --all-extras
```

### Issue: Spark session initialization fails

```bash
# Solution: Check Databricks Connect is configured (for real Spark)
# For local testing, conftest.py provides a local Spark session automatically
```

### Issue: Test passes locally but fails in CI

- CI runs without Databricks infrastructure
- Local Spark session is used (not DatabricksSession)
- Ensure tests don't depend on specific Databricks features
- Use mocking if needed for Databricks-specific APIs

### Issue: Memory errors with large test data

```python
# Solution: Reduce data size or use repartition
data = [...]  # Smaller dataset
df = spark.createDataFrame(data).coalesce(1)
```

## Best Practices

### Writing Integration Tests

1. **Use fixtures for reusable data** - `@pytest.fixture` for test DataFrames
2. **Test one transformation per test** - Keep tests focused
3. **Use realistic data** - Mirror actual schema and values
4. **Assert the right things** - Check both count and content
5. **Name tests descriptively** - Test name should explain what's being tested
6. **Avoid external dependencies** - Tests should be self-contained

### Performance

- **Cache DataFrames** if used multiple times: `df.cache()`
- **Use small datasets** for fast feedback
- **Partition data** appropriately: `df.repartition(1)`

### Organization

- One test class per component/layer
- Group related tests together
- Use descriptive class and method names
- Document complex test logic

## Extending Tests

### Adding new tests for a transformation

1. Create test method in appropriate test class
2. Use `@pytest.mark.integration` marker
3. Create fixture for test data
4. Apply the transformation
5. Assert the results

Example:

```python
@pytest.mark.integration
class TestNewFeature:
    
    @pytest.fixture
    def input_data(self, spark):
        return spark.createDataFrame([...])
    
    def test_new_transformation(self, spark, input_data):
        result = input_data.transform(...)
        assert result.count() == expected
```

### Validating new Silver transformations

1. Create test in `test_silver_layer_spark.py`
2. Add fixture for Bronze input data
3. Apply window and aggregation logic
4. Verify output schema and values
5. Test edge cases (nulls, empty, etc.)

### Testing new Gold metrics

1. Create test in `test_gold_layer_spark.py`
2. Join relevant Silver tables
3. Apply calculation logic
4. Verify numerical accuracy
5. Test data quality constraints

## CI/CD Integration

Integration tests run in GitHub Actions:

- **Unit tests**: Always run (fast, no dependencies)
- **Integration tests**: Optional (requires Spark, slower)
- **Local Spark**: Uses default SparkSession (not DatabricksSession)

To run integration tests in CI:

```bash
uv run pytest tests/integration -v -m integration
```

## References

- [Pytest Documentation](https://docs.pytest.org)
- [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.functions.html)
- [Window Functions](https://spark.apache.org/docs/latest/sql-ref-window-functions.html)
