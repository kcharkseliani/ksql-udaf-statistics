package com.kcharkseliani.kafka.ksql.statistics;

import com.kcharkseliani.kafka.ksql.statistics.util.UdafMetadata;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.*;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.Network;

public class AllUdafIT {

    static KafkaContainer kafka;
    static GenericContainer<?> ksqldb;

    static Network network;

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

    @Test
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

    @Test
    void testStddevWeighted_RecordsInserted_ShouldAggregateAll() throws Exception {
        String host = ksqldb.getHost();
        int port = ksqldb.getMappedPort(8088);
        String baseUrl = "http://" + host + ":" + port + "/ksql";

        HttpClient client = HttpClient.newHttpClient();

        // === Input test data ===
        double[] values = {5.0, 2.0, 8.0};
        double[] weights = {2.0, 4.0, 1.0};

        // === Expected weighted stddev computation ===
        double weightedSum = 0.0, weightedSumSquares = 0.0, sumWeights = 0.0;
        for (int i = 0; i < values.length; i++) {
            weightedSum += values[i] * weights[i];
            weightedSumSquares += weights[i] * Math.pow(values[i], 2);
            sumWeights += weights[i];
        }
        double mean = weightedSum / sumWeights;
        double variance = (weightedSumSquares / sumWeights) - Math.pow(mean, 2);
        double expectedStdDev = Math.sqrt(Math.max(variance, 0.0));

        // === 1. Create stream ===
        String createStream =
            "{ \"ksql\": \"CREATE STREAM weighted_input (val DOUBLE, weight DOUBLE) " +
            "WITH (kafka_topic='weighted_input', value_format='json', partitions=1);\", " +
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

        // === 2. Create table with aggregation using STDDEV_WEIGHTED ===
        // Sleep to wait for stream creation to complete
        Thread.sleep(2_000);
        String createTableWithSingletonKey =
            "{ \"ksql\": \"CREATE TABLE stddev_result WITH (" +
            "KAFKA_TOPIC='stddev_output', PARTITIONS=1, VALUE_FORMAT='JSON') AS " +
            "SELECT 'singleton' AS id, STDDEV_WEIGHTED(val, weight) AS stddev " +
            "FROM weighted_input " +
            "GROUP BY 'singleton' EMIT CHANGES;\"," +
            " \"streamsProperties\": {} }";

        HttpResponse<String> tableResponse = client.send(
            HttpRequest.newBuilder()
                .uri(URI.create(baseUrl))
                .header("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(createTableWithSingletonKey))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );

        Assertions.assertEquals(200, tableResponse.statusCode(), 
            "Failed to create the table with the UDAF result: " + tableResponse.body());

        // === 3. Insert test data ===
        // Sleep to wait for table creation to complete
        Thread.sleep(2_000);
        
        // Build INSERT statements dynamically
        StringBuilder insertStatements = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            insertStatements.append("INSERT INTO weighted_input (val, weight) VALUES (")
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

        // === 4. Query table result ===
        // Sleep to wait for data to finish inserting
        Thread.sleep(2_000);

        String pullQuery =
        "{ \"ksql\": \"SELECT * FROM stddev_result WHERE id = 'singleton';\", " +
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

        // === 5. Extract and compare value ===
        String body = queryResponse.body();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(body);

        // The second element of the array contains the actual row with results
        JsonNode rowNode = root.get(1).path("row").path("columns");

        // Get the standard deviation (second column)
        double actualStdDev = rowNode.get(1).asDouble();

        Assertions.assertEquals(expectedStdDev, actualStdDev, 0.0001);
    }

    @AfterAll
    static void tearDown() {
        ksqldb.stop();
        kafka.stop();
    }
}
