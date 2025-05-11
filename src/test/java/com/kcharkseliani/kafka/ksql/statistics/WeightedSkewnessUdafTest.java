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
 * Unit tests for the {@link WeightedSkewnessUdaf} class.
 * 
 * This class validates that the custom weighted skewness UDAF (user-defined aggregate function)
 * behaves correctly across all of its expected operations: initialization, aggregation, mapping,
 * and merging of intermediate states. It also verifies correct behavior when handling zero weights.
 */
public class WeightedSkewnessUdafTest {

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

    /** Schema used to structure the aggregation result. */
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
            .field(SUM_VALUES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHTS, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHT_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(SUM_WEIGHT_CUBES, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    /**
     * Initializes a new instance of the UDAF before each test.
     */
    @BeforeEach
    void setUp() {
        udafImpl = WeightedSkewnessUdaf.createUdaf();
    }

    /**
     * Tests that the {@code initialize} method creates a struct with all zero fields.
     */
    @Test
    void testInitialize_ShouldContainZeroedState() {
        Struct initialStruct = udafImpl.initialize();

        assertNotNull(initialStruct);
        assertEquals(0.0, initialStruct.getFloat64(SUM_VALUES));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHTS));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHT_SQUARES));
        assertEquals(0.0, initialStruct.getFloat64(SUM_WEIGHT_CUBES));
    }

    /**
     * Tests that the {@code aggregate} method correctly updates the intermediate aggregation state.
     */
    @Test
    void testAggregate_ShouldUpdateIntermediateStateCorrectly() {
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

    /**
     * Tests that the {@code map} method correctly computes the weighted skewness.
     */
    @Test
    void testMap_ValidRecords_ShouldReturnExpectedSkewness() {
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

    /**
     * Tests that the {@code merge} method combines two intermediate structs correctly.
     */
    @Test
    void testMerge_ShouldCombineIntermediateStatesCorrectly() {
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

    /**
     * Tests that {@code map} returns 0 when weights are all zero, avoiding division by zero.
     */
    @Test
    void testMap_ZeroWeights_ShouldReturnZeroSkewness() {
        Struct aggregate = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 0.0)
                .put(SUM_WEIGHTS, 0.0)
                .put(SUM_WEIGHT_SQUARES, 0.0)
                .put(SUM_WEIGHT_CUBES, 0.0);

        Double skewness = udafImpl.map(aggregate);

        assertEquals(0.0, skewness, 0.0001);
    }

    /**
     * Tests that {@code map} returns 0 when variance is zero,
     * even if weights are non-zero (e.g. all values are the same).
     */
    @Test
    void testMap_ZeroVariance_ShouldReturnZeroSkewness() {
        // All values are 3.0, weights are non-zero, so variance = 0
        double repeatedValue = 3.0;
        double totalWeight = 6.0;
        double sumValues = repeatedValue * totalWeight;
        double sumWeightSquares = repeatedValue * repeatedValue * totalWeight;
        double sumWeightCubes = repeatedValue * repeatedValue * repeatedValue * totalWeight;

        Struct aggregate = new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, sumValues)
                .put(SUM_WEIGHTS, totalWeight)
                .put(SUM_WEIGHT_SQUARES, sumWeightSquares)
                .put(SUM_WEIGHT_CUBES, sumWeightCubes);

        Double skewness = udafImpl.map(aggregate);

        // Expect skewness to be zero since variance is zero (all values equal)
        assertEquals(0.0, skewness, 0.0001);
    }
}

