package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class WeightedSkewnessUdafTest {

    private Udaf<Pair<Double, Double>, Struct, Double> udafImpl;
    private static final String SUM_VALUES = "SUM_VALUES";
    private static final String SUM_WEIGHTS = "SUM_WEIGHTS";
    private static final String SUM_WEIGHT_SQUARES = "SUM_WEIGHT_SQUARES";
    private static final String SUM_WEIGHT_CUBES = "SUM_WEIGHT_CUBES";
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
            .field(SUM_VALUES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHTS, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHT_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHT_CUBES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    @BeforeEach
    void setUp() {
        udafImpl = WeightedSkewnessUdaf.createUdaf();
    }

    @Test
    void testInitialize() {
        Struct initialStruct = udafImpl.initialize();

        assertNotNull(initialStruct);
        assertEquals(0.0, initialStruct.getFloat64(SUM_VALUES));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHTS));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHT_SQUARES));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHT_CUBES));
    }

    @Test
    void testAggregate() {
        Pair<Double, Double> pair = new Pair<>(5.0, 2.0);
        Struct aggregateStruct = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 10.0)
                .put(SUM_WEIGHTS, 4.0)
                .put(SUM_WEIGHT_SQUARES, 50.0)
                .put(SUM_WEIGHT_CUBES, 250.0);

        Struct result = udafImpl.aggregate(pair, aggregateStruct);

        assertNotNull(result);
        assertEquals(10.0 + 5.0 * 2.0, result.getFloat64(SUM_VALUES), 0.0001);
        assertEquals(4.0 + 2.0, result.getFloat64(SUM_WEIGHTS), 0.0001);
        assertEquals(50.0 + 2.0 * Math.pow(5.0, 2), result.getFloat64(SUM_WEIGHT_SQUARES), 0.0001);
        assertEquals(250.0 + 2.0 * Math.pow(5.0, 3), result.getFloat64(SUM_WEIGHT_CUBES), 0.0001);
    }

    @Test
    void testMap() {
        Struct aggregate = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 15.0)
                .put(SUM_WEIGHTS, 5.0)
                .put(SUM_WEIGHT_SQUARES, 55.0)
                .put(SUM_WEIGHT_CUBES, 225.0);

        Double skewness = udafImpl.map(aggregate);

        double mean = 15.0 / 5.0;
        double variance = (55.0 / 5.0) - Math.pow(mean, 2);
        double expectedSkewness = ((225.0 / 5.0) - 3 * mean * (55.0 / 5.0) + 2 * Math.pow(mean, 3))
                / Math.pow(Math.max(variance, 0.0), 1.5);

        assertEquals(expectedSkewness, skewness, 0.0001);
    }

    @Test
    void testMerge() {
        Struct aggOne = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 20.0)
                .put(SUM_WEIGHTS, 6.0)
                .put(SUM_WEIGHT_SQUARES, 70.0)
                .put(SUM_WEIGHT_CUBES, 280.0);

        Struct aggTwo = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 30.0)
                .put(SUM_WEIGHTS, 8.0)
                .put(SUM_WEIGHT_SQUARES, 120.0)
                .put(SUM_WEIGHT_CUBES, 480.0);

        Struct merged = udafImpl.merge(aggOne, aggTwo);

        assertNotNull(merged);
        assertEquals(50.0, merged.getFloat64(SUM_VALUES), 0.0001);
        assertEquals(14.0, merged.getFloat64(SUM_WEIGHTS), 0.0001);
        assertEquals(190.0, merged.getFloat64(SUM_WEIGHT_SQUARES), 0.0001);
        assertEquals(760.0, merged.getFloat64(SUM_WEIGHT_CUBES), 0.0001);
    }

    @Test
    void testMapWithZeroWeights() {
        Struct aggregate = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 0.0)
                .put(SUM_WEIGHTS, 0.0)
                .put(SUM_WEIGHT_SQUARES, 0.0)
                .put(SUM_WEIGHT_CUBES, 0.0);

        Double skewness = udafImpl.map(aggregate);

        assertEquals(0.0, skewness, 0.0001);
    }
}

