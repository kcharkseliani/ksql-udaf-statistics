// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Konstantin Charkseliani

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

import java.util.Arrays;

/**
 * Unit tests for the {@link WeightedStdDevUdaf} class.
 * 
 * This class verifies the correctness of the custom weighted standard deviation UDAF (user-defined aggregate function)
 * in ksqlDB. It covers the lifecycle operations of the UDAF, including initialization, aggregation, mapping, 
 * merging of intermediate states, and handling of edge cases like zero weights.
 */
public class WeightedStdDevUdafTest {

    /** Instance of the UDAF under test. */
    private Udaf<Pair<Double, Double>, Struct, Double> udafImpl;

    /** Field name for the internal aggregation state of the sum of values. */
    private static final String SUM_VALUES = "SUM_VALUES";

    /** Field name for the internal aggregation state of the sum of weights. */
    private static final String SUM_WEIGHTS = "SUM_WEIGHTS";

    /** Field name for the internal aggregation state of the sum of weighted squares. */
    private static final String SUM_WEIGHT_SQUARES = "SUM_WEIGHT_SQUARES";

    /** Schema used to structure the aggregation state. */
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
            .field(SUM_VALUES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHTS, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHT_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    /**
     * Initializes the UDAF instance before each test.
     */
    @BeforeEach
    void setUp() {
        udafImpl = WeightedStdDevUdaf.createUdaf();
    }

    /**
     * Tests that the {@code initialize} method returns a struct with zeroed fields.
     */
    @Test
    void testInitialize_ShouldContainZeroedState() {
        Struct initialStruct = udafImpl.initialize();

        assertNotNull(initialStruct);
        assertEquals(0.0, initialStruct.getFloat64(SUM_VALUES));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHTS));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHT_SQUARES));
    }

    /**
     * Tests that the {@code aggregate} method correctly updates aggregation state for a given input.
     */
    @Test
    void testAggregate_ShouldUpdateIntermediateStateCorrectly() {
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

    /**
     * Tests that the {@code map} method correctly calculates the weighted standard deviation.
     */
    @Test
    void testMap_ValidRecords_ShouldReturnExpectedStdDev() {

        // Using values and weights for a known result
        double[] values = { 3.0, 4.0, 7.0, 13.0, 16.0, 20.0 };
        double[] weights = { 2.0, 1.0, 2.0, 1.0, 3.0, 1.0 };

        double sumValues = 0.0;
        double sumWeights = 0.0;
        double sumWeightSquares = 0.0;

        // Compute sums that are used by the map method
        for (int i = 0; i < values.length; i++) {
            double value = values[i];
            double weight = weights[i];

            sumValues += value * weight;
            sumWeights += weight;
            sumWeightSquares += weight * Math.pow(value, 2);
        }

        // Create a mock Struct with aggregated values
        Struct aggregate = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, sumValues)
                .put(SUM_WEIGHTS, sumWeights)
                .put(SUM_WEIGHT_SQUARES, sumWeightSquares);

        // Perform the map operation (final calculation)
        Double stddev = udafImpl.map(aggregate);

        // Validate the result
        assertEquals(6.0539, stddev, 0.0001);
    }

    /**
     * Tests that the {@code merge} method correctly combines two intermediate aggregation structs.
     */
    @Test
    void testMerge_ShouldCombineIntermediateStatesCorrectly() {
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
        assertEquals(25.0, merged.getFloat64(SUM_VALUES), 0.0001);
        assertEquals(9.0, merged.getFloat64(SUM_WEIGHTS), 0.0001);
        assertEquals(50.0, merged.getFloat64(SUM_WEIGHT_SQUARES), 0.0001); 
    }

    /**
     * Tests that the {@code map} method returns zero when all weights are zero, avoiding division by zero.
     */
    @Test
    void testMap_ZeroWeights_ShouldReturnZeroStdDev() {
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
