package com.kcharkseliani.kafka.ksql.statistics;

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
    static void setUp() {

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
            .withEnv("KSQL_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
            .withEnv("KSQL_KSQL_EXTENSION_DIR", "/opt/ksqldb-udfs")  // mount your UDAF JAR here
            .withFileSystemBind("extensions", "/opt/ksqldb-udfs", BindMode.READ_ONLY) // mounts your UDAF uber-jar
            .dependsOn(kafka);

        ksqldb.start();
    }

    @Test
    void testKsqlWithCustomUdaf() throws Exception {
        throw new UnsupportedOperationException("Method testKsqlWithCustomUdaf not implemented");
    }

    @AfterAll
    static void tearDown() {
        ksqldb.stop();
        kafka.stop();
    }
}
