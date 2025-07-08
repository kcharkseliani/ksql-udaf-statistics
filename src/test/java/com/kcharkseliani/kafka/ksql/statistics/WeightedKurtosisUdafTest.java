// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Konstantin Charkseliani

package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the {@link WeightedKurtosisUdaf} class.
 *
 * This class verifies correct behavior of the weighted kurtosis UDAF implementation,
 * covering initialization, aggregation, mapping, merging of intermediates states,
 * and numerical edge cases. It also verifies correct behavior when handling zero weights.
 */
public class WeightedKurtosisUdafTest {

    /** Instance of the UDAF under test. */
    private Udaf<Pair<Double, Double>, Struct, Double> udafImpl;

    /** Field name for the internal aggregation state of the sum of values. */
    private static final String SUM_VALUES = "SUM_VALUES";

    /** Field name for the internal aggregation state of the sum of weights. */
    private static final String SUM_WEIGHTS = "SUM_WEIGHTS";

    /** Field name for the internal aggregation state of the sum of weighted squares. */
    private static final String SUM_WEIGHT_SQUARES = "SUM_WEIGHT_SQUARES";

    /** Field name for the internal aggregation state of the sum of weighted cubes. */
    private static final String SUM_WEIGHT_CUBES = "SUM_WEIGHT_CUBES";

    /** Field name for the internal aggregation state of the sum of weighted quartic powers. */
    private static final String SUM_WEIGHT_QUARTIC = "SUM_WEIGHT_QUARTIC";

    /** Schema used to structure the aggregation result. */
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
        .field(SUM_VALUES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHTS, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHT_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHT_CUBES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHT_QUARTIC, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    /**
     * Initializes a new instance of the UDAF before each test.
     */
    @BeforeEach
    void setUp() {
        udafImpl = WeightedKurtosisUdaf.createUdaf();
    }

    /**
     * Tests that the {@code initialize} method creates a struct with all zero fields.
     */
    @Test
    void testInitialize_ShouldContainZeroedState() {

        Struct s = udafImpl.initialize();

        assertEquals(0.0, s.getFloat64(SUM_VALUES));
        assertEquals(0.0, s.getFloat64(SUM_WEIGHTS));
        assertEquals(0.0, s.getFloat64(SUM_WEIGHT_SQUARES));
        assertEquals(0.0, s.getFloat64(SUM_WEIGHT_CUBES));
        assertEquals(0.0, s.getFloat64(SUM_WEIGHT_QUARTIC));
    }

    /**
     * Tests that the {@code aggregate} method correctly updates the intermediate aggregation state.
     */
    @Test
    void testAggregate_ShouldUpdateIntermediateStateCorrectly() {

        Struct s = new Struct(STRUCT_SCHEMA)
            .put(SUM_VALUES, 10.0)
            .put(SUM_WEIGHTS, 4.0)
            .put(SUM_WEIGHT_SQUARES, 30.0)
            .put(SUM_WEIGHT_CUBES, 80.0)
            .put(SUM_WEIGHT_QUARTIC, 150.0);

        Pair<Double, Double> input = new Pair<>(3.0, 2.0);

        Struct updated = udafImpl.aggregate(input, s);

        assertEquals(10.0 + 2.0 * 3.0, updated.getFloat64(SUM_VALUES), 0.0001);
        assertEquals(4.0 + 2.0, updated.getFloat64(SUM_WEIGHTS), 0.0001);
        assertEquals(30.0 + 2.0 * 9.0, updated.getFloat64(SUM_WEIGHT_SQUARES), 0.0001);
        assertEquals(80.0 + 2.0 * 27.0, updated.getFloat64(SUM_WEIGHT_CUBES), 0.0001);
        assertEquals(150.0 + 2.0 * 81.0, updated.getFloat64(SUM_WEIGHT_QUARTIC), 0.0001);
    }

    /**
     * Tests that the {@code map} method correctly computes the weighted kurtosis.
     */
    @Test
    void testMap_ValidRecords_ShouldReturnExpectedKurtosis() {

        // Using values and weights for a known result
        double[] values = { 3.0, 4.0, 7.0, 13.0, 16.0, 20.0 };
        double[] weights = { 2.0, 1.0, 2.0, 1.0, 3.0, 1.0 };

        double sumValues = 0.0;
        double sumWeights = 0.0;
        double sumWeightSquares = 0.0;
        double sumWeightCubes = 0.0;
        double sumWeightQuartic = 0.0;

        // Compute sums that are used by the map method
        for (int i = 0; i < values.length; i++) {
            double value = values[i];
            double weight = weights[i];

            sumValues += value * weight;
            sumWeights += weight;
            sumWeightSquares += weight * Math.pow(value, 2);
            sumWeightCubes += weight * Math.pow(value, 3);
            sumWeightQuartic += weight * Math.pow(value, 4);
        }

        Struct aggregate = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, sumValues)
                .put(SUM_WEIGHTS, sumWeights)
                .put(SUM_WEIGHT_SQUARES, sumWeightSquares)
                .put(SUM_WEIGHT_CUBES, sumWeightCubes)
                .put(SUM_WEIGHT_QUARTIC, sumWeightQuartic);

        assertEquals(1.4400, udafImpl.map(aggregate), 0.0001);
    }

    /**
     * Tests that {@code map} returns 0 when weights are all zero, avoiding division by zero.
     */
    @Test
    void testMap_ZeroWeights_ShouldReturnZeroKurtosis() {

        Struct s = new Struct(STRUCT_SCHEMA)
            .put(SUM_VALUES, 0.0)
            .put(SUM_WEIGHTS, 0.0)
            .put(SUM_WEIGHT_SQUARES, 0.0)
            .put(SUM_WEIGHT_CUBES, 0.0)
            .put(SUM_WEIGHT_QUARTIC, 0.0);

        assertEquals(0.0, udafImpl.map(s), 0.0001);
    }

    /**
     * Tests that {@code map} returns 0 when variance is zero,
     * even if weights are non-zero (e.g. all values are the same).
     */
    @Test
    void testMap_ZeroVariance_ShouldReturnZero() {

        double value = 4.0;
        double weight = 3.0;
        Struct s = new Struct(STRUCT_SCHEMA)
            .put(SUM_VALUES, value * weight)
            .put(SUM_WEIGHTS, weight)
            .put(SUM_WEIGHT_SQUARES, Math.pow(value, 2) * weight)
            .put(SUM_WEIGHT_CUBES, Math.pow(value, 3) * weight)
            .put(SUM_WEIGHT_QUARTIC, Math.pow(value, 4) * weight);

        assertEquals(0.0, udafImpl.map(s), 0.0001);
    }

    /**
     * Tests that the {@code merge} method combines two intermediate structs correctly.
     */
    @Test
    void testMerge_ShouldCombineAggregatesCorrectly() {

        Struct a = new Struct(STRUCT_SCHEMA)
            .put(SUM_VALUES, 10.0)
            .put(SUM_WEIGHTS, 2.0)
            .put(SUM_WEIGHT_SQUARES, 40.0)
            .put(SUM_WEIGHT_CUBES, 80.0)
            .put(SUM_WEIGHT_QUARTIC, 160.0);

        Struct b = new Struct(STRUCT_SCHEMA)
            .put(SUM_VALUES, 20.0)
            .put(SUM_WEIGHTS, 3.0)
            .put(SUM_WEIGHT_SQUARES, 50.0)
            .put(SUM_WEIGHT_CUBES, 90.0)
            .put(SUM_WEIGHT_QUARTIC, 170.0);

        Struct result = udafImpl.merge(a, b);

        assertEquals(10.0 + 20.0, result.getFloat64(SUM_VALUES), 0.0001);
        assertEquals(2.0 + 3.0, result.getFloat64(SUM_WEIGHTS), 0.0001);
        assertEquals(40.0 + 50.0, result.getFloat64(SUM_WEIGHT_SQUARES), 0.0001);
        assertEquals(80.0 + 90.0, result.getFloat64(SUM_WEIGHT_CUBES), 0.0001);
        assertEquals(160.0 + 170.0, result.getFloat64(SUM_WEIGHT_QUARTIC), 0.0001);
    }
}