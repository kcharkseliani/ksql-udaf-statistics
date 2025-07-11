// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Konstantin Charkseliani

package com.kcharkseliani.kafka.ksql.statistics;

import com.kcharkseliani.kafka.ksql.statistics.util.UdafMetadata;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;  
import java.util.Properties;
import java.time.Duration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.Network;

/**
 * Integration test suite for verifying custom UDAFs (User-Defined Aggregate Functions)
 * in a ksqlDB environment using Testcontainers.
 * 
 * This test class deploys a temporary Kafka and ksqlDB instance,
 * loads UDAF extensions, inserts test records, and verifies
 * correct aggregation results using both HTTP queries and Kafka consumers.
 */
@TestMethodOrder(OrderAnnotation.class)
public class AllUdafIT {

    /**
     * Testcontainers-managed Kafka broker used for integration testing.
     */
    static KafkaContainer kafka;

    /**
     * Testcontainers-managed ksqlDB server instance used for executing queries and aggregations.
     */
    static GenericContainer<?> ksqldb;

    /**
     * Shared Docker network for inter-container communication between Kafka and ksqlDB.
     */
    static Network network;

    /**
     * Bootstraps Testcontainers for Kafka and ksqlDB before all tests run.
     * Ensures the UDAF extension JAR is mounted and ksqlDB is configured correctly.
     */
    @BeforeAll
    static void setUp() throws Exception {

        boolean dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        System.out.println("Testcontainers Docker available: " + dockerAvailable);

        if (!dockerAvailable) {
            throw new IllegalStateException("Docker is not available. Integration tests require Docker.");
        }

        network = Network.newNetwork();
        
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
        .withNetwork(network)
        .withNetworkAliases("kafka")
        .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092")
        .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092");

        kafka.start();

        ksqldb = new GenericContainer<>(DockerImageName.parse("confluentinc/ksqldb-server:0.29.0"))
            .withNetwork(network)
            .withExposedPorts(8088)
            .withNetworkAliases("ksqldb")
            .withEnv("KSQL_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
            .withEnv("KSQL_KSQL_EXTENSION_DIR", "/opt/ksqldb-udfs")  // mount your UDAF JAR here
            .withFileSystemBind("extensions", "/opt/ksqldb-udfs", BindMode.READ_ONLY) // mounts your UDAF uber-jar
            .dependsOn(kafka);

        ksqldb.start();

        // Sleep to wait for ksqlDB to be ready for requests
        Thread.sleep(5_000);
    }

    /**
     * Verifies that all UDAFs defined in the codebase are correctly registered and discoverable in ksqlDB.
     * Uses annotation scanning via {@link UdafMetadata}. Always runs first using {@link OrderAnnotation}
     *
     * @throws Exception if the HTTP query or parsing fails
     */
    @Test
    @Order(1)
    void testDeployment_ShouldContainAllDeclaredUdafs() throws Exception {

        Set<String> expectedFunctionNames = UdafMetadata.getDeclaredUdafNames();
        System.out.println("Expected UDAF names from annotations: " + expectedFunctionNames);

        // Query ksqlDB for actual functions
        String host = ksqldb.getHost();
        int port = ksqldb.getMappedPort(8088);

        String payload = "{\n" +
            "  \"ksql\": \"SHOW FUNCTIONS;\",\n" +
            "  \"streamsProperties\": {}\n" +
            "}";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + host + ":" + port + "/ksql"))
                .header("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response.body());

        Set<String> actualFunctionNames = StreamSupport.stream(root.spliterator(), false)
                .flatMap(node -> StreamSupport.stream(node.path("functions").spliterator(), false))
                .map(fn -> fn.path("name").asText().toLowerCase())
                .collect(Collectors.toSet());

        for (String expected : expectedFunctionNames) {
            Assertions.assertTrue(actualFunctionNames.contains(expected),
                "Missing UDAF in ksqlDB: " + expected);
        }
    }

    /**
     * Tests the STDDEV_WEIGHTED UDAF on a set of weighted values.
     * Asserts that the standard deviation value matches the expected computation.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testStddevWeighted_ValidRecordsInserted_ShouldAggregateAll() throws Exception {

        double[] values = {5.0, 2.0, 8.0};
        double[] weights = {2.0, 4.0, 1.0};

        double expected = computeWeightedStdDev(values, weights);

        runAggregationTest(List.of(values, weights), 
            expected, 
            "STDDEV_WEIGHTED");
    }

    /**
     * Tests the STDDEV_WEIGHTED UDAF on all-zero values and weights.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testStddevWeighted_AllZeroRecordsInserted_ShouldReturnZero() throws Exception {

        double[] values = {0, 0, 0};
        double[] weights = {0, 0, 0};

        double expected = computeWeightedStdDev(values, weights);

        runAggregationTest(List.of(values, weights), 
            expected, 
            "STDDEV_WEIGHTED");
    }

    /**
     * Tests the SKEWNESS UDAF on a representative dataset.
     * Asserts that the skewness value matches the expected computation.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testSkewness_ValidRecordsInserted_ShouldAggregateAll() throws Exception {

        double[] values = {4.0, 7.0, 13.0, 16.0, 20.0};

        double expected = computePopulationSkewness(values);

        runAggregationTest(
            List.of(values),     // Only values column (no weights)
            expected,
            "SKEWNESS"
        );
    }

    /**
     * Tests the SKEWNESS UDAF with bias correction (sample skewness) on a representative dataset.
     * Asserts that the skewness value matches the expected computation.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testSkewness_WithBiasCorrection_ShouldAggregateAll() throws Exception {

        double[] values = {4.0, 7.0, 13.0, 16.0, 20.0};

        // This computes sample skewness
        double expected = new Skewness().evaluate(values);

        runAggregationTest(
            List.of(values),     // Only values column (no weights)
            expected,
            "SKEWNESS",
            true
        );
    }

    /**
     * Tests the SKEWNESS_WEIGHTED UDAF on a representative weighted dataset.
     * Asserts that the skewness value matches the expected computation.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testSkewnessWeighted_ValidRecordsInserted_ShouldAggregateAll() throws Exception {

        double[] values = {5.0, 2.0, 8.0};
        double[] weights = {2.0, 4.0, 1.0};

        double expected = computeWeightedSkewness(values, weights);

        runAggregationTest(List.of(values, weights), 
            expected, 
            "SKEWNESS_WEIGHTED");
    }

    /**
     * Tests the SKEWNESS UDAF with bias correction and < 3 samples.
     * Asserts that the result is NaN.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testSkewness_InsufficientDataForSample_ShouldReturnNaN() throws Exception {

        double[] values = {1, 2};

        runAggregationTest(List.of(values), 
            Double.NaN, 
            "SKEWNESS",
            true);
    }

    /**
     * Tests the SKEWNESS_WEIGHTED UDAF where all weights are zero.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testSkewnessWeighted_AllZeroRecordsInserted_ShouldReturnZero() throws Exception {

        double[] values = {0, 0, 0};
        double[] weights = {0, 0, 0};

        double expected = computeWeightedSkewness(values, weights);

        runAggregationTest(List.of(values, weights), 
            expected, 
            "SKEWNESS_WEIGHTED");
    }

    /**
     * Tests the SKEWNESS UDAF on values with zero variance.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testSkewness_ZeroVarianceRecordsInserted_ShouldReturnZero() throws Exception {

        double[] values = {1, 1, 1};

        runAggregationTest(List.of(values), 
            0.0, 
            "SKEWNESS");
    }

    /**
     * Tests the SKEWNESS_WEIGHTED UDAF on values with zero variance.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testSkewnessWeighted_ZeroVarianceRecordsInserted_ShouldReturnZero() throws Exception {

        double[] values = {0, 0, 0};
        double[] weights = {1, 1, 1};

        double expected = computeWeightedSkewness(values, weights);

        runAggregationTest(List.of(values, weights), 
            expected, 
            "SKEWNESS_WEIGHTED");
    }

    /**
     * Tests the KURTOSIS UDAF on a representative dataset.
     * Asserts that the kurtosis value matches the expected computation.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testKurtosis_ValidRecordsInserted_ShouldAggregateAll() throws Exception {

        double[] values = {14.0, 7.0, 13.0, 16.0, 20.0, 15.0};

        double expected = computePopulationKurtosis(values);

        runAggregationTest(
            List.of(values),
            expected,
            "KURTOSIS"
        );
    }

    /**
     * Tests the KURTOSIS UDAF with bias correction (sample kurtosis) on a representative dataset.
     * Asserts that the kurtosis value matches the expected computation.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testKurtosis_WithBiasCorrection_ShouldAggregateAll() throws Exception {

        double[] values = {14.0, 7.0, 13.0, 16.0, 20.0, 15.0};

        // This computes sample skewness
        double expected = new Kurtosis().evaluate(values);

        runAggregationTest(
            List.of(values),
            expected,
            "KURTOSIS",
            true
        );
    }

    /**
     * Tests the KURTOSIS_WEIGHTED UDAF on a representative weighted dataset.
     * Asserts that the kurtosis value matches the expected computation.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testKurtosisWeighted_ValidRecordsInserted_ShouldAggregateAll() throws Exception {
        double[] values = {5.0, 2.0, 8.0, 4.0};
        double[] weights = {2.0, 4.0, 1.0, 2.0};

        double expected = computeWeightedKurtosis(values, weights);

        runAggregationTest(List.of(values, weights),
            expected,
            "KURTOSIS_WEIGHTED");
    }

    /**
     * Tests the KURTOSIS UDAF with bias correction and < 4 samples.
     * Asserts that the result is NaN.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testKurtosis_InsufficientDataForSample_ShouldReturnNaN() throws Exception {

        double[] values = {1, 2};

        runAggregationTest(List.of(values), 
            Double.NaN, 
            "KURTOSIS",
            true);
    }

    /**
     * Tests the KURTOSIS_WEIGHTED UDAF where all weights are zero.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testKurtosisWeighted_AllZeroRecordsInserted_ShouldReturnZero() throws Exception {
        double[] values = {0.0, 0.0, 0.0};
        double[] weights = {0.0, 0.0, 0.0};

        double expected = computeWeightedKurtosis(values, weights);

        runAggregationTest(List.of(values, weights),
            expected,
            "KURTOSIS_WEIGHTED");
    }

    /**
     * Tests the KURTOSIS UDAF on values with zero variance.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testKurtosis_ZeroVarianceRecordsInserted_ShouldReturnZero() throws Exception {

        double[] values = {1, 1, 1, 1};

        runAggregationTest(List.of(values), 
            0.0, 
            "KURTOSIS");
    }

    /**
     * Tests the KURTOSIS_WEIGHTED UDAF on values with zero variance.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if any HTTP requests or parsing operations fail
     */
    @Test
    void testKurtosisWeighted_ZeroVarianceRecordsInserted_ShouldReturnZero() throws Exception {
        double[] values = {3.0, 3.0, 3.0, 3.0};
        double[] weights = {1.0, 1.0, 1.0, 1.0};

        double expected = computeWeightedKurtosis(values, weights);

        runAggregationTest(List.of(values, weights),
            expected,
            "KURTOSIS_WEIGHTED");
    }

    /**
     * Cleans up the test environment after each test method by dropping created tables and streams.
     * This ensures isolated test execution and fresh state.
     *
     * @throws Exception if cleanup HTTP requests fail
     */
    @AfterEach
    void cleanUpKsqlDbObjects() throws Exception {
        String host = ksqldb.getHost();
        int port = ksqldb.getMappedPort(8088);
        String baseUrl = "http://" + host + ":" + port + "/ksql";
        HttpClient client = HttpClient.newHttpClient();

        // Drop table if exists
        String dropTable = "{ \"ksql\": \"DROP TABLE IF EXISTS aggregated_result DELETE TOPIC;\", \"streamsProperties\": {} }";
        HttpResponse<String> tableResponse = client.send(
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl))
            .header("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(dropTable))
            .build(),
        HttpResponse.BodyHandlers.ofString()
        );
        if (tableResponse.statusCode() != 200) {
            throw new IllegalStateException("Failed to drop table: " + tableResponse.body());
        }

        Thread.sleep(2_000);

        // Drop stream if exists
        String dropStream = "{ \"ksql\": \"DROP STREAM IF EXISTS input_values DELETE TOPIC;\", \"streamsProperties\": {} }";
        HttpResponse<String> streamResponse = client.send(
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl))
            .header("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(dropStream))
            .build(),
        HttpResponse.BodyHandlers.ofString()
        );
        if (streamResponse.statusCode() != 200) {
            throw new IllegalStateException("Failed to drop stream: " + streamResponse.body());
        }

        Thread.sleep(2_000);
    }

    /**
     * Tears down and stops all running Testcontainers including Kafka and ksqlDB
     * after all tests have completed.
     */
    @AfterAll
    static void tearDown() {
        ksqldb.stop();
        kafka.stop();
    }

    /**
     * Helper method that executes a full integration test against a specified UDAF.
     * Works with an arbitrary number of input columns for the function.
     * It creates a stream and table, inserts test data, and verifies results through:
     * - Pull queries to ksqlDB
     * - Reading from the output Kafka topic
     *
     * @param columnValues List of arrays, each representing the values for a column (must be the same length)
     * @param expectedValue The expected aggregation result for the specified function
     * @param functionName The registered name of the UDAF function to invoke (e.g., "SKEWNESS_WEIGHTED")
     * @param constructorArgs Optional arguments passed to the UDAF constructor (e.g., flags like isSample)
     * @throws Exception If any step in the test flow fails (stream creation, data insertion, or verification)
     */
    private void runAggregationTest(
        List<double[]> columnValues,
        double expectedValue,
        String functionName,
        Object... constructorArgs) throws Exception {

        // Building a comma-separated string of optional constructor arguments
        String argLiterals = Stream.of(constructorArgs)
            .map(arg -> {
                if (arg instanceof String) return "'" + arg + "'";
                else if (arg instanceof Boolean) return (Boolean) arg ? "true" : "false";
                else return String.valueOf(arg);
            })
            .collect(Collectors.joining(", "));

        // Making sure the numbers of values in each column supplied are the same
        int recordCount = columnValues.get(0).length;
        for (double[] col : columnValues) {
            if (col.length != recordCount) {
                throw new IllegalArgumentException("All value arrays must be the same length");
            }
        }

        // Auto-generate column names: col1, col2, col3, ...
        List<String> columnNames = IntStream.range(0, columnValues.size())
            .mapToObj(i -> "col" + (i + 1))
            .collect(Collectors.toList());

        String host = ksqldb.getHost();
        int port = ksqldb.getMappedPort(8088);
        String baseUrl = "http://" + host + ":" + port + "/ksql";
    
        HttpClient client = HttpClient.newHttpClient();
    
        // === 1. Create stream ===
        String streamColumns = columnNames.stream()
            .map(col -> col + " DOUBLE")
            .collect(Collectors.joining(", "));

        String createStream = String.format(
            "{ \"ksql\": \"CREATE STREAM input_values (%s) " +
            "WITH (kafka_topic='input_values', value_format='json', partitions=1);\", " +
            "\"streamsProperties\": {} }",
            streamColumns
        );
    
        HttpResponse<String> streamResponse = client.send(
            HttpRequest.newBuilder()
                .uri(URI.create(baseUrl))
                .header("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(createStream))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
    
        if (streamResponse.statusCode() != 200) {
            throw new IllegalStateException("Failed to create stream: " + streamResponse.body());
        }
    
        // Sleep to wait for stream creation to complete
        Thread.sleep(2_000);
    
        // === 2. Create table with aggregation using specified UDAF ===
        String functionArgs = String.join(", ", columnNames);

        // If constructor args exist, append them
        if (!argLiterals.isEmpty()) {
            functionArgs += ", " + argLiterals;
        }

        String createTable = String.format(
            "{ \"ksql\": \"CREATE TABLE aggregated_result " +
            "WITH (KAFKA_TOPIC='aggregated_output', PARTITIONS=1, VALUE_FORMAT='JSON') AS " +
            "SELECT 'singleton' AS id, %s(%s) AS result " +
            "FROM input_values GROUP BY 'singleton' EMIT CHANGES;\", " +
            "\"streamsProperties\": {} }",
            functionName, functionArgs
        );
    
        HttpResponse<String> tableResponse = client.send(
            HttpRequest.newBuilder()
                .uri(URI.create(baseUrl))
                .header("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(createTable))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
    
        Assertions.assertEquals(200, tableResponse.statusCode(),
            "Failed to create the table with the UDAF result: " + tableResponse.body());
    
        // Sleep to wait for table creation to complete
        Thread.sleep(5_000);
    
        // === 3. Insert data ===
        // Build INSERT statements dynamically
        StringBuilder insertStatements = new StringBuilder();
        for (int i = 0; i < recordCount; i++) {
            final int rowIndex = i;

            insertStatements.append("INSERT INTO input_values (")
                .append(String.join(", ", columnNames))
                .append(") VALUES (")
                .append(IntStream.range(0, columnNames.size())
                                .mapToObj(j -> String.valueOf(columnValues.get(j)[rowIndex]))
                                .collect(Collectors.joining(", ")))
                .append("); ");
        }
    
        // Build the final JSON string
        String insertData = "{\n" +
            "  \"ksql\": \"" + insertStatements.toString().trim() + "\",\n" +
            "  \"streamsProperties\": {}\n" +
            "}";
    
        HttpResponse<String> insertResponse = client.send(
            HttpRequest.newBuilder()
                .uri(URI.create(baseUrl))
                .header("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(insertData))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
    
        if (insertResponse.statusCode() != 200) {
            throw new IllegalStateException("Failed to insert test data: " + insertResponse.body());
        }
    
        // Sleep to wait for data to finish inserting
        Thread.sleep(5_000);
    
        // === 4. Query and validate result ===
        String pullQuery = "{ \"ksql\": \"SELECT * FROM aggregated_result WHERE id = 'singleton';\", " +
            "\"streamsProperties\": {} }";
    
        HttpResponse<String> queryResponse = client.send(
            HttpRequest.newBuilder()
                .uri(URI.create("http://" + host + ":" + port + "/query"))
                .header("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(pullQuery))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
    
        Assertions.assertEquals(200, queryResponse.statusCode(), 
            "Pull query to extract UDAF result failed: " + queryResponse.body());
    
        JsonNode root = new ObjectMapper().readTree(queryResponse.body());       
        JsonNode resultValue = root.get(1).path("row").path("columns").get(1);

        double actual;

        // A 'null' in ksqlDB becomes NaN for validation purposes
        if (resultValue.isNull()) {
            actual = Double.NaN;
        } else {
            actual = resultValue.asDouble();
        }
    
        if (Double.isNaN(expectedValue)) {
            Assertions.assertTrue(Double.isNaN(actual), "Expected NaN but got " + actual);
        } 
        else {
            Assertions.assertEquals(expectedValue, actual, 0.0001, 
                "Expected " + expectedValue + " but got " + actual);
        }

        // === 5. Consume from output Kafka topic to verify ===
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("aggregated_output"));

            boolean found = false;
            double actualFromKafka = Double.NaN;
            long start = System.currentTimeMillis();

            // Poll Kafka for new messages until timeout expires
            while (!found && System.currentTimeMillis() - start < 5_000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    if (value.contains("RESULT")) {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode json = mapper.readTree(value);
                        JsonNode resultNode = json.path("RESULT");

                        // A 'null' in Kafka becomes NaN for validation purposes
                        if (resultNode.isNull()) {
                            actualFromKafka = Double.NaN;
                        } 
                        else {
                            actualFromKafka = resultNode.asDouble();
                        }
                        
                        found = true;
                        break;
                    }
                }
            }

            Assertions.assertTrue(found, "Did not receive aggregated output from Kafka topic");
            if (Double.isNaN(expectedValue)) {
                Assertions.assertTrue(Double.isNaN(actualFromKafka), "Expected NaN from Kafka but got " + actualFromKafka);
            } 
            else {
                Assertions.assertEquals(expectedValue, actualFromKafka, 0.0001, 
                    "Expected value from Kafka " + expectedValue + " but got " + actualFromKafka);
            }
        }
    }

    /**
     * Computes the weighted standard deviation of a set of values using provided weights.
     *
     * @param values array of numeric values
     * @param weights array of corresponding weights
     * @return the weighted standard deviation, or 0.0 if total weight is zero
     */
    private static double computeWeightedStdDev(double[] values, double[] weights) {

        double weightedSum = 0.0;
        double weightedSumSquares = 0.0;
        double sumWeights = 0.0;
    
        for (int i = 0; i < values.length; i++) {
            weightedSum += values[i] * weights[i];
            weightedSumSquares += weights[i] * Math.pow(values[i], 2);
            sumWeights += weights[i];
        }
    
        // Avoid division by zero
        if (sumWeights == 0.0) {
            return 0.0;
        }
    
        double mean = weightedSum / sumWeights;
        double variance = (weightedSumSquares / sumWeights) - Math.pow(mean, 2);
        return Math.sqrt(Math.max(variance, 0.0));
    }

    /**
     * Computes the weighted skewness of a set of values using provided weights.
     *
     * @param values array of numeric values
     * @param weights array of corresponding weights
     * @return the weighted skewness, or 0.0 if total weight or variance is zero
     */
    private static double computeWeightedSkewness(double[] values, double[] weights) {

        double sumWeights = 0.0;
        double weightedSum = 0.0;
        double weightedSumSquares = 0.0;
        double weightedSumCubes = 0.0;
    
        for (int i = 0; i < values.length; i++) {
            double val = values[i];
            double weight = weights[i];
    
            sumWeights += weight;
            weightedSum += weight * val;
            weightedSumSquares += weight * Math.pow(val, 2);
            weightedSumCubes += weight * Math.pow(val, 3);
        }
    
        // Avoid division by zero
        if (sumWeights == 0.0) {
            return 0.0;
        }
    
        double mean = weightedSum / sumWeights;
        double variance = (weightedSumSquares / sumWeights) - Math.pow(mean, 2);
        double m3 = (weightedSumCubes / sumWeights) - 3 * mean * (weightedSumSquares / sumWeights) + 2 * Math.pow(mean, 3);
    
        // Avoid division by zero
        if (variance == 0.0) {
            return 0.0;
        }
    
        return m3 / Math.pow(variance, 1.5);
    }

    /**
     * Computes the population skewness of a set of values.
     * @param values array of numeric values
     * @return the population skewness, or 0.0 if the variance is zero or input is empty
     */
    private static double computePopulationSkewness(double[] values) {

        int n = values.length;
        if (n == 0) {
            return 0.0;
        }
    
        double mean = Arrays.stream(values).average().orElse(0.0);
        double variance = 0.0, m3 = 0.0;
    
        for (double x : values) {
            double diff = x - mean;
            variance += diff * diff;
            m3 += diff * diff * diff;
        }
    
        variance /= n;
        m3 /= n;
    
        return variance == 0 ? 0.0 : m3 / Math.pow(variance, 1.5);
    }

    /**
     * Computes the population kurtosis of a set of values.
     * @param values array of numeric values
     * @return the population kurtosis, or 0.0 if the variance is zero or input is empty
     */
    private static double computePopulationKurtosis(double[] values) {

        int n = values.length;
        if (n == 0) {
            return 0.0;
        }

        double mean = Arrays.stream(values).average().orElse(0.0);
        double variance = 0.0, m4 = 0.0;

        for (double x : values) {
            double diff = x - mean;
            double diffSq = diff * diff;
            variance += diffSq;
            // (x - mean)^4
            m4 += diffSq * diffSq;
        }

        variance /= n;
        m4 /= n;

        return variance == 0 ? 0.0 : m4 / (variance * variance);
    }

    /**
     * Computes the weighted kurtosis of a set of values using provided weights.
     *
     * @param values array of numeric values
     * @param weights array of corresponding weights
     * @return the weighted kurtosis, or 0.0 if total weight or variance is zero
     */
    private static double computeWeightedKurtosis(double[] values, double[] weights) {

        double sumWeights = 0.0;
        double sum = 0.0;
        double variance = 0.0;
        double m4 = 0.0;

        for (int i = 0; i < values.length; i++) {
            double x = values[i];
            double w = weights[i];
            sumWeights += w;
            sum += w * x;
        }

        if (sumWeights == 0.0) {
            return 0.0;
        }

        double mean = sum / sumWeights;

        for (int i = 0; i < values.length; i++) {
            double x = values[i];
            double w = weights[i];
            double diff = x - mean;
            variance += w * diff * diff;
            m4 += w * Math.pow(diff, 4);
        }

        variance /= sumWeights;
        m4 /= sumWeights;

        return variance == 0.0 ? 0.0 : m4 / (variance * variance);
    }
}