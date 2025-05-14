package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the {@link SkewnessUdaf} class.
 *
 * This class validates that the skewness UDAF (user-defined aggregate function) 
 * behaves correctly across all of its expected operations:
 * initialization, aggregation, mapping, and merging. It also verifies behavior under edge conditions.
 */
public class SkewnessUdafTest {

    /** Instance of the skewness UDAF under test. */
    private Udaf<Double, Struct, Double> udafImpl;

    /** Instance of the sample skewness UDAF under test. */
    private Udaf<Double, Struct, Double> udafImplSample;

    /** Field name for the internal aggregation state of the number of observations. */
    private static final String COUNT = "COUNT";

    /** Field name for the internal aggregation state of the sum of values. */
    private static final String SUM = "SUM";

    /** Field name for the internal aggregation state of the sum of squared values. */
    private static final String SUM_SQUARES = "SUM_SQUARES";

    /** Field name for the internal aggregation state of the sum of cubed values. */
    private static final String SUM_CUBES = "SUM_CUBES";

    /** Schema used to represent the aggregation state. */
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
        .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
        .field(SUM, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_CUBES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    /** Initializes a new UDAF instance before each test. */
    @BeforeEach
    void setUp() {
        udafImpl = SkewnessUdaf.createUdaf();
        udafImplSample = SkewnessUdaf.createUdaf(true);
    }

    /**
     * Verifies that the {@code initialize} method returns a struct with all zero fields.
     */
    @Test
    void testInitialize_ShouldContainZeroedState() {

        Struct initial = udafImpl.initialize();

        assertEquals(0L, initial.getInt64(COUNT));
        assertEquals(0.0, initial.getFloat64(SUM));
        assertEquals(0.0, initial.getFloat64(SUM_SQUARES));
        assertEquals(0.0, initial.getFloat64(SUM_CUBES));
    }

    /**
     * Verifies that {@code aggregate} correctly updates intermediate state given a new value.
     */
    @Test
    void testAggregate_ShouldUpdateIntermediateStateCorrectly() {

        Struct current = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 2L)
            .put(SUM, 6.0)
            .put(SUM_SQUARES, 20.0)
            .put(SUM_CUBES, 70.0);

        Double input = 4.0;

        long expectedCount = 2L + 1;
        double expectedSum = 6.0 + input;
        double expectedSumSquares = 20.0 + Math.pow(input, 2);
        double expectedSumCubes = 70.0 + Math.pow(input, 3);

        Struct result = udafImpl.aggregate(input, current);

        assertEquals(expectedCount, result.getInt64(COUNT));
        assertEquals(expectedSum, result.getFloat64(SUM), 0.0001);
        assertEquals(expectedSumSquares, result.getFloat64(SUM_SQUARES), 0.0001);
        assertEquals(expectedSumCubes, result.getFloat64(SUM_CUBES), 0.0001);
    }

    /**
     * Verifies that {@code map} returns the correct skewness for valid data.
     */
    @Test
    void testMap_ValidData_ShouldReturnExpectedSkewness() {

        Struct aggregate = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 5L)
            .put(SUM, 60.0)
            .put(SUM_SQUARES, 890.0)
            .put(SUM_CUBES, 14700.0);

        Double result = udafImpl.map(aggregate);

        double mean = 60.0 / 5;
        double variance = (890.0 / 5) - Math.pow(mean, 2);
        double m3 = (14700.0 / 5) - 3 * mean * (890.0 / 5) + 2 * Math.pow(mean, 3);
        double expected = m3 / Math.pow(variance, 1.5);

        assertEquals(expected, result, 0.0001);
    }

    @Test
    void testMap_WithSampleCorrection_ShouldReturnCorrectSampleSkewness() {

        Struct aggregate = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 5L)
            .put(SUM, 60.0)
            .put(SUM_SQUARES, 890.0)
            .put(SUM_CUBES, 14700.0);

        double count = 5;
        double mean = 60.0 / count;
        double variance = ((890.0 / count) - Math.pow(mean, 2)) * (count / (count - 1.0));
        double m3 = (14700.0 / count) - 3 * mean * (890.0 / count) + 2 * Math.pow(mean, 3);
        double expectedSkew = m3 / Math.pow(variance, 1.5) * 
            (count * count) / ((count - 1.0) * (count - 2.0));

        double result = udafImplSample.map(aggregate);
        assertEquals(expectedSkew, result, 0.0001);
    }

    /**
     * Verifies that {@code map} returns 0.0 if the count is zero.
     */
    @Test
    void testMap_ZeroCount_ShouldReturnZero() {

        Struct aggregate = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 0L)
            .put(SUM, 0.0)
            .put(SUM_SQUARES, 0.0)
            .put(SUM_CUBES, 0.0);

        assertEquals(0.0, udafImpl.map(aggregate), 0.0001);
    }

    /**
     * Verifies that {@code map} returns NaN for sample skewness if count < 3.
     */
    @Test
    void testMap_InsufficientDataForSample_ShouldReturnNaN() {

        Struct aggregate = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 2L)               // Less than 3
            .put(SUM, 10.0)
            .put(SUM_SQUARES, 52.0)
            .put(SUM_CUBES, 260.0);

        double result = udafImplSample.map(aggregate);

        assertTrue(Double.isNaN(result), "Expected NaN for sample skewness with count < 3");
    }

    /**
     * Verifies that {@code map} returns 0 if the variance is zero.
     */
    @Test
    void testMap_ZeroVariance_ShouldReturnZero() {

        Struct aggregate = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 3L)
            .put(SUM, 9.0)
            .put(SUM_SQUARES, 3.0 * 3.0 * 3.0) // 3 * 3^2 = 27
            .put(SUM_CUBES, 3.0 * 3.0 * 3.0 * 3.0); // 3 * 3^3 = 81

        assertEquals(0.0, udafImpl.map(aggregate), 0.0001);
    }

    /**
     * Verifies that {@code merge} correctly combines two aggregation states.
     */
    @Test
    void testMerge_ShouldCombineIntermediateStatesCorrectly() {

        Struct aggOne = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 2L)
            .put(SUM, 6.0)
            .put(SUM_SQUARES, 20.0)
            .put(SUM_CUBES, 70.0);

        Struct aggTwo = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 1L)
            .put(SUM, 4.0)
            .put(SUM_SQUARES, 16.0)
            .put(SUM_CUBES, 64.0);

        Struct merged = udafImpl.merge(aggOne, aggTwo);
        assertEquals(2L + 1L, merged.getInt64(COUNT));
        assertEquals(6.0 + 4.0, merged.getFloat64(SUM), 0.0001);
        assertEquals(20.0 + 16.0, merged.getFloat64(SUM_SQUARES), 0.0001);
        assertEquals(70.0 + 64.0, merged.getFloat64(SUM_CUBES), 0.0001);
    }
}