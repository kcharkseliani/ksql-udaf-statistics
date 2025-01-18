package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class WeightedStdDevUdafTest {

    private Udaf<Pair<Double, Double>, Struct, Double> udafImpl;
    private static final String SUM_VALUES = "SUM_VALUES";
    private static final String SUM_WEIGHTS = "SUM_WEIGHTS";
    private static final String SUM_WEIGHT_SQUARES = "SUM_WEIGHT_SQUARES";
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
            .field(SUM_VALUES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHTS, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHT_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    @BeforeEach
    void setUp() {
        udafImpl = WeightedStdDevUdaf.createUdaf();
    }

    @Test
    void testInitialize() {
        Struct initialStruct = udafImpl.initialize();

        assertNotNull(initialStruct);
        assertEquals(0.0, initialStruct.getFloat64(SUM_VALUES));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHTS));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHT_SQUARES));
    }

    @Test
    void testAggregate() {
        // Create a mock Pair<Double, Double> for value and weight
        Pair<Double, Double> pair = new Pair<>(5.0, 2.0); // value = 5.0, weight = 2.0
        Struct aggregateStruct = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 10.0)
                .put(SUM_WEIGHTS, 4.0)
                .put(SUM_WEIGHT_SQUARES, 20.0);

        // Perform aggregation
        Struct result = udafImpl.aggregate(pair, aggregateStruct);

        // Validate the result
        assertNotNull(result);
        assertEquals(10.0 + 5.0 * 2.0, result.getFloat64(SUM_VALUES), 0.0001);  // SUM_VALUES
        assertEquals(4.0 + 2.0, result.getFloat64(SUM_WEIGHTS), 0.0001);  // SUM_WEIGHTS
        assertEquals(20.0 + 2.0 * Math.pow(5.0, 2), result.getFloat64(SUM_WEIGHT_SQUARES), 0.0001);  // SUM_WEIGHT_SQUARES
    }

    @Test
    void testMap() {
        // Create a mock Struct with aggregated values
        Struct aggregate = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 20.0)
                .put(SUM_WEIGHTS, 6.0)
                .put(SUM_WEIGHT_SQUARES, 50.0);

        // Perform the map operation (final calculation)
        Double stddev = udafImpl.map(aggregate);

        // Calculate expected standard deviation manually
        double weightedMean = 20.0 / 6.0;
        double variance = (50.0 / 6.0) - Math.pow(weightedMean, 2);
        double expectedStdDev = Math.sqrt(Math.max(variance, 0.0));

        // Validate the result
        assertEquals(expectedStdDev, stddev, 0.0001);
    }

    @Test
    void testMerge() {
        // Create two mock Struct objects
        Struct aggOne = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 10.0)
                .put(SUM_WEIGHTS, 4.0)
                .put(SUM_WEIGHT_SQUARES, 20.0);

        Struct aggTwo = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 15.0)
                .put(SUM_WEIGHTS, 5.0)
                .put(SUM_WEIGHT_SQUARES, 30.0);

        // Perform the merge operation
        Struct merged = udafImpl.merge(aggOne, aggTwo);

        // Validate the merged result
        assertNotNull(merged);
        assertEquals(25.0, merged.getFloat64(SUM_VALUES), 0.0001);  // SUM_VALUES
        assertEquals(9.0, merged.getFloat64(SUM_WEIGHTS), 0.0001);  // SUM_WEIGHTS
        assertEquals(50.0, merged.getFloat64(SUM_WEIGHT_SQUARES), 0.0001);  // SUM_WEIGHT_SQUARES
    }

    @Test
    void testMapWithZeroWeights() {
        // Create a mock Struct with zero weights
        Struct aggregate = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 0.0)
                .put(SUM_WEIGHTS, 0.0)
                .put(SUM_WEIGHT_SQUARES, 0.0);

        // Perform the map operation (final calculation)
        Double stddev = udafImpl.map(aggregate);

        // Validate the result: with zero weights, standard deviation should be 0
        assertEquals(0.0, stddev, 0.0001);
    }
}
