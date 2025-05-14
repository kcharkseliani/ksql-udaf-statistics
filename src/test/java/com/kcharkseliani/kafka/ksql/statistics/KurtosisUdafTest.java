package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;

import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

/**
 * Unit tests for the {@link KurtosisUdaf} class.
 *
 * This class validates that the kurtosis UDAF behaves correctly across all expected operations:
 * initialization, aggregation, mapping, and merging. It also verifies behavior for edge conditions
 * such as zero count, insufficient data for sample kurtosis, and zero variance.
 */
public class KurtosisUdafTest {

    private Udaf<Double, Struct, Double> udafImpl;
    private Udaf<Double, Struct, Double> udafImplSample;

    private static final String COUNT = "COUNT";
    private static final String SUM = "SUM";
    private static final String SUM_SQUARES = "SUM_SQUARES";
    private static final String SUM_CUBES = "SUM_CUBES";
    private static final String SUM_QUARTIC = "SUM_QUARTIC";

    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
        .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
        .field(SUM, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_CUBES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_QUARTIC, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    @BeforeEach
    void setUp() {
        udafImpl = KurtosisUdaf.createUdaf();
        udafImplSample = KurtosisUdaf.createUdaf(true);
    }

    @Test
    void testInitialize_ShouldReturnZeroedStruct() {
        Struct s = udafImpl.initialize();
        assertEquals(0L, s.getInt64(COUNT));
        assertEquals(0.0, s.getFloat64(SUM));
        assertEquals(0.0, s.getFloat64(SUM_SQUARES));
        assertEquals(0.0, s.getFloat64(SUM_CUBES));
        assertEquals(0.0, s.getFloat64(SUM_QUARTIC));
    }

    @Test
    void testAggregate_ShouldUpdateIntermediateStateCorrectly() {
        Struct current = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 2L)
            .put(SUM, 4.0)
            .put(SUM_SQUARES, 10.0)
            .put(SUM_CUBES, 28.0)
            .put(SUM_QUARTIC, 82.0);

        double input = 3.0;

        Struct result = udafImpl.aggregate(input, current);

        assertEquals(3L, result.getInt64(COUNT));
        assertEquals(4.0 + input, result.getFloat64(SUM), 0.0001);
        assertEquals(10.0 + Math.pow(input, 2), result.getFloat64(SUM_SQUARES), 0.0001);
        assertEquals(28.0 + Math.pow(input, 3), result.getFloat64(SUM_CUBES), 0.0001);
        assertEquals(82.0 + Math.pow(input, 4), result.getFloat64(SUM_QUARTIC), 0.0001);
    }

    @Test
    void testMap_PopulationKurtosis_ShouldReturnCorrectValue() {
        Struct s = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 5L)
            .put(SUM, 15.0)
            .put(SUM_SQUARES, 55.0)
            .put(SUM_CUBES, 225.0)
            .put(SUM_QUARTIC, 979.0);

        double mean = 15.0 / 5;
        double variance = (55.0 / 5) - Math.pow(mean, 2);
        double m4 = (979.0 / 5) - 4 * mean * (225.0 / 5) + 6 
            * Math.pow(mean, 2) * (55.0 / 5) - 3 * Math.pow(mean, 4);
        double expected = m4 / Math.pow(variance, 2);

        double actual = udafImpl.map(s);
        assertEquals(expected, actual, 0.0001);
    }

    @Test
    void testMap_WithSampleCorrection_ShouldReturnCorrectSampleKurtosis() {
        double[] values = {1.0, 2.0, 3.0, 4.0, 5.0};

        // Compute expected using Apache Commons Math
        Kurtosis kurtosis = new Kurtosis(); // by default, bias correction (sample kurtosis) is applied
        double expected = kurtosis.evaluate(values);

        // Build aggregate manually
        double sum = Arrays.stream(values).sum();
        double sumSquares = Arrays.stream(values).map(v -> v * v).sum();
        double sumCubes = Arrays.stream(values).map(v -> Math.pow(v, 3)).sum();
        double sumQuartic = Arrays.stream(values).map(v -> Math.pow(v, 4)).sum();

        Struct s = new Struct(STRUCT_SCHEMA)
            .put(COUNT, (long) values.length)
            .put(SUM, sum)
            .put(SUM_SQUARES, sumSquares)
            .put(SUM_CUBES, sumCubes)
            .put(SUM_QUARTIC, sumQuartic);

        double actual = udafImplSample.map(s);
        assertEquals(expected, actual, 0.0001);
    }

    @Test
    void testMap_ZeroCount_ShouldReturnZero() {
        Struct s = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 0L)
            .put(SUM, 0.0)
            .put(SUM_SQUARES, 0.0)
            .put(SUM_CUBES, 0.0)
            .put(SUM_QUARTIC, 0.0);

        assertEquals(0.0, udafImpl.map(s), 0.0001);
    }

    @Test
    void testMap_InsufficientDataForSample_ShouldReturnNaN() {
        Struct s = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 3L) // less than 4 for sample
            .put(SUM, 9.0)
            .put(SUM_SQUARES, 27.0)
            .put(SUM_CUBES, 81.0)
            .put(SUM_QUARTIC, 243.0);

        assertTrue(Double.isNaN(udafImplSample.map(s)));
    }

    @Test
    void testMap_ZeroVariance_ShouldReturnZero() {
        double repeated = 5.0;
        double count = 4.0;
        double sum = repeated * count;
        double sumSq = repeated * repeated * count;
        double sumCb = Math.pow(repeated, 3) * count;
        double sumQ = Math.pow(repeated, 4) * count;

        Struct s = new Struct(STRUCT_SCHEMA)
            .put(COUNT, (long) count)
            .put(SUM, sum)
            .put(SUM_SQUARES, sumSq)
            .put(SUM_CUBES, sumCb)
            .put(SUM_QUARTIC, sumQ);

        assertEquals(0.0, udafImpl.map(s), 0.0001);
    }

    @Test
    void testMerge_ShouldCombineIntermediateStatesCorrectly() {
        Struct a = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 2L)
            .put(SUM, 10.0)
            .put(SUM_SQUARES, 50.0)
            .put(SUM_CUBES, 250.0)
            .put(SUM_QUARTIC, 1250.0);

        Struct b = new Struct(STRUCT_SCHEMA)
            .put(COUNT, 3L)
            .put(SUM, 12.0)
            .put(SUM_SQUARES, 50.0)
            .put(SUM_CUBES, 216.0)
            .put(SUM_QUARTIC, 962.0);

        Struct result = udafImpl.merge(a, b);

        assertEquals(5L, result.getInt64(COUNT));
        assertEquals(22.0, result.getFloat64(SUM), 0.0001);
        assertEquals(100.0, result.getFloat64(SUM_SQUARES), 0.0001);
        assertEquals(466.0, result.getFloat64(SUM_CUBES), 0.0001);
        assertEquals(2212.0, result.getFloat64(SUM_QUARTIC), 0.0001);
    }
}
