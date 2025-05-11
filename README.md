# ksql-udaf-statistics

A collection of custom **ksqlDB User-Defined Aggregate Functions (UDAFs)** that perform additional basic and advanced statistical calculations on aggregated data.

## Features

- Implements `STDDEV_WEIGHTED` and `SKEWNESS_WEIGHTED` UDAFs.
- Supports weighted aggregation on streaming values using ksqlDB.
- Designed to work with `ksqldb-server` via UDF extensions.
- Includes unit tests and integration tests using Testcontainers.
- Compatible with ksqlDB 0.29.0 and Kafka 7.8.0.

## Use Cases

- Calculate real-time statistical metrics (beyond those available in ksqlDB) over event streams (e.g., financial data, telemetry).

## Quickstart

### 1. Install Gradle
This project was built using Gradle 8.11.1. Please ensure that Gradle is installed on your system before proceeding.
You can verify your installation with:
```bash
gradle --version
```
> **Note:** This project does not include a Gradle wrapper (```gradlew```). You must install Gradle manually.

### 1. Build the UDAF Extension JAR

```
gradle shadowJar
```

This builds the uber-JAR including all required dependencies and places it at:

```
extensions/ksql-udaf-statistics-<version>.jar
```

### 2. Run With ksqlDB

Mount the generated JAR as an extension in ksqlDB:

```bash
docker run -it --rm \
  -v /path/to/your/repo/extensions:/opt/ksqldb-udfs \
  -e KSQL_KSQL_EXTENSION_DIR=/opt/ksqldb-udfs \
  -p 8088:8088 \
  confluentinc/ksqldb-server:0.29.0
```

### 3. Use in ksqlDB

```sql
-- Create a stream
CREATE STREAM input (val DOUBLE, weight DOUBLE)
  WITH (KAFKA_TOPIC='input', VALUE_FORMAT='json');

-- Aggregate using the UDAF
CREATE TABLE agg_result WITH (
  KAFKA_TOPIC='output',
  KEY_FORMAT='JSON'
) AS
SELECT
  'singleton' AS id,
   STDDEV_WEIGHTED(val, weight) AS stddev,
   SKEWNESS_WEIGHTED(val, weight) AS skewness
FROM input
GROUP BY 'singleton';
```
You can also apply these UDAFs in a window (e.g. tumbling) to compute aggregates over time periods:
```sql
-- Time-windowed aggregation example
CREATE TABLE agg_result_windowed WITH (
  KAFKA_TOPIC = 'output',
  KEY_FORMAT = 'JSON'
) AS
SELECT
  'singleton' AS id,
  WINDOWSTART AS window_start,
  STDDEV_WEIGHTED(val, weight) AS stddev,
  SKEWNESS_WEIGHTED(val, weight) AS skewness
FROM input
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY 'singleton';
```
> **Note:** ```'singleton'``` is used as a constant key to satisfy the ```GROUP BY``` requirement for tables in ksqlDB. You can replace it with an actual column name if grouping by real data fields.

## Development

This project uses:

- Java 11
- Gradle with Shadow Plugin
- Confluent's ksqlDB UDF API (`ksqldb-udf`, `ksqldb-common`)
- Apache Kafka (`kafka_2.13`)
- Testcontainers for integration testing

### Run All Tests and Build Uber-JAR
```
gradle verify
```

### Run Unit Tests
```
gradle test
```

### Build Uber-JAR
This includes all required dependencies for ksqlDB server.
```
gradle shadowJar
```

### Run Only Integration Tests
This will use Testcontainers to spin up Kafka and ksqlDB server w/ mounted UDF extension to test functions end-to-end.
```
gradle integrationTest
```

> **Note:** Docker must be running for integration tests.

> **Note:** The latest uber-JAR must be built using ```shadowJar``` before running integration tests.

## Project Structure

- `WeightedStdDevUdaf`: UDAF for weighted standard deviation.
- `WeightedSkewnessUdaf`: UDAF for weighted skewness.
- `AllUdafIT`: Integration test validating UDAF behavior with real ksqlDB and Kafka.
- `WeightedStdDevUdafTest` / `WeightedSkewnessUdafTest`: Unit tests for aggregation logic.
- `UdafMetadata`: Utility to extract registered UDAF function names via annotations.

## Publishing

Release artifacts are automatically created by the GitHub Actions pipeline:
- A release JAR is built using Gradle (```shadowJar```) and placed in ```extensions/```.
- The pipeline uploads this JAR to a **GitHub Release**, tagged with the current version.
- Draft releases are created as stable for the main branch, and as prerelease for other branches.
- Detailed release description (following the format of previous releases) must be added manually before publishing.

## License

MIT License. See [LICENSE](./LICENSE) for details.
