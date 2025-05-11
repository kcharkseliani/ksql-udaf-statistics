package com.kcharkseliani.kafka.ksql.statistics;

import com.kcharkseliani.kafka.ksql.statistics.util.UdafMetadata;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.Collections;
import java.util.Properties;
import java.time.Duration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestMethodOrder;

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
    void testDeployment_shouldContainAllDeclaredUdafs() throws Exception {

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
     * @throws Exception if any ksqlDB or HTTP operation fails
     */
    @Test
    void testStddevWeighted_ValidRecordsInserted_ShouldAggregateAll() throws Exception {

        double[] values = {5.0, 2.0, 8.0};
        double[] weights = {2.0, 4.0, 1.0};

        double expected = computeWeightedStdDev(values, weights);

        runWeightedAggregationTest(values, weights, expected, "STDDEV_WEIGHTED");
    }

    /**
     * Tests the STDDEV_WEIGHTED UDAF on all-zero values and weights.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if the test setup or query fails
     */
    @Test
    void testStddevWeighted_AllZeroRecordsInserted_ShouldReturnZero() throws Exception {

        double[] values = {0, 0, 0};
        double[] weights = {0, 0, 0};

        double expected = computeWeightedStdDev(values, weights);

        runWeightedAggregationTest(values, weights, expected, "STDDEV_WEIGHTED");
    }

    /**
     * Tests the SKEWNESS_WEIGHTED UDAF on a representative weighted dataset.
     * Asserts that the skewness value matches the expected computation.
     *
     * @throws Exception if any HTTP or parsing operation fails
     */
    @Test
    void testSkewnessWeighted_ValidRecordsInserted_ShouldAggregateAll() throws Exception {

        double[] values = {5.0, 2.0, 8.0};
        double[] weights = {2.0, 4.0, 1.0};

        double expected = computeWeightedSkewness(values, weights);

        runWeightedAggregationTest(values, weights, expected, "SKEWNESS_WEIGHTED");
    }

    /**
     * Tests the SKEWNESS_WEIGHTED UDAF where all weights are zero.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if setup or validation fails
     */
    @Test
    void testSkewnessWeighted_AllZeroRecordsInserted_ShouldReturnZero() throws Exception {

        double[] values = {0, 0, 0};
        double[] weights = {0, 0, 0};

        double expected = computeWeightedSkewness(values, weights);

        runWeightedAggregationTest(values, weights, expected, "SKEWNESS_WEIGHTED");
    }

    /**
     * Tests the SKEWNESS_WEIGHTED UDAF on values with zero variance.
     * Asserts that the result is zero and does not produce NaN or error.
     *
     * @throws Exception if ksqlDB interaction fails
     */
    @Test
    void testSkewnessWeighted_ZeroVarianceRecordsInserted_ShouldReturnZero() throws Exception {

        double[] values = {0, 0, 0};
        double[] weights = {1, 1, 1};

        double expected = computeWeightedSkewness(values, weights);

        runWeightedAggregationTest(values, weights, expected, "SKEWNESS_WEIGHTED");
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
     * It creates a stream and table, inserts test data, and verifies results through:
     * - Pull queries to ksqlDB
     * - Reading from the output Kafka topic
     *
     * @param values array of input values
     * @param weights corresponding array of weights
     * @param expectedValue expected result from the aggregation
     * @param functionName the name of the UDAF to apply (e.g. STDDEV_WEIGHTED)
     * @throws Exception if any step in the test flow fails
     */
    private void runWeightedAggregationTest(double[] values, 
        double[] weights, 
        double expectedValue, 
        String functionName) throws Exception {

        String host = ksqldb.getHost();
        int port = ksqldb.getMappedPort(8088);
        String baseUrl = "http://" + host + ":" + port + "/ksql";
    
        HttpClient client = HttpClient.newHttpClient();
    
        // === 1. Create stream ===
        String createStream =
            "{ \"ksql\": \"CREATE STREAM input_values (val DOUBLE, weight DOUBLE) " +
            "WITH (kafka_topic='input_values', value_format='json', partitions=1);\", " +
            "\"streamsProperties\":{} }";
    
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
        String createTable =
            "{ \"ksql\": \"CREATE TABLE aggregated_result WITH (" +
            "KAFKA_TOPIC='aggregated_output', PARTITIONS=1, VALUE_FORMAT='JSON') AS " +
            "SELECT 'singleton' AS id, " + functionName + "(val, weight) AS result " +
            "FROM input_values " +
            "GROUP BY 'singleton' EMIT CHANGES;\"," +
            " \"streamsProperties\": {} }";
    
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
        for (int i = 0; i < values.length; i++) {
            insertStatements.append("INSERT INTO input_values (val, weight) VALUES (")
                            .append(values[i]).append(", ").append(weights[i]).append("); ");
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
        double actual = resultValue.asDouble();
    
        Assertions.assertEquals(expectedValue, actual, 0.0001,
            "Expected " + expectedValue + " but got " + actual);

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
                        actualFromKafka = json.path("RESULT").asDouble();
                        found = true;
                        break;
                    }
                }
            }

            Assertions.assertTrue(found, "Did not receive aggregated output from Kafka topic");
            Assertions.assertEquals(expectedValue, actualFromKafka, 0.0001,
                "Expected value from Kafka " + expectedValue + " but got " + actualFromKafka);
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
        double m2 = (weightedSumSquares / sumWeights) - Math.pow(mean, 2);
        double m3 = (weightedSumCubes / sumWeights) - 3 * mean * (weightedSumSquares / sumWeights) + 2 * Math.pow(mean, 3);
    
        // Avoid division by zero
        if (m2 == 0.0) {
            return 0.0;
        }
    
        return m3 / Math.pow(m2, 1.5);
    }
}