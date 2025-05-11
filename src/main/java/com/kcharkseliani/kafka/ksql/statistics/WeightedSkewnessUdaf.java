package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * A user-defined aggregate function (UDAF) for computing weighted skewness in ksqlDB.
 * 
 * This UDAF accepts pairs of (value, weight) and maintains a running calculation of
 * weighted sums, squares, and cubes to compute skewness on streaming data.
 * 
 * Registered under the function name {@code skewness_weighted}.
 * 
 * This class is stateless and delegates all UDAF logic to its internal 
 * {@link WeightedSkewnessUdafImpl} implementation class.
 */
@UdafDescription(name = "skewness_weighted",
                 author = "Konstantin Charkseliani",
                 version = "0.1.0",
                 description = "Calculates skewness based on weights of each value")
public class WeightedSkewnessUdaf {

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

    /** Private constructor to prevent instantiation. */
    private WeightedSkewnessUdaf() {
    }

    /**
     * Factory method to register the weighted skewness UDAF with ksqlDB.
     *
     * @return a new instance of the UDAF implementation
     */
    @UdafFactory(description = "Calculates the weighted skewness of a stream of values with weights.",
            aggregateSchema = "STRUCT<SUM_VALUES double, SUM_WEIGHTS double, SUM_WEIGHT_SQUARES double, SUM_WEIGHT_CUBES double>")
    public static Udaf<Pair<Double, Double>, Struct, Double> createUdaf() {
        return new WeightedSkewnessUdafImpl();
    }

    /**
     * Implementation of the weighted skewness UDAF logic.
     * 
     * Aggregates a stream of (value, weight) pairs into a struct and calculates
     * weighted skewness during mapping or merges two aggregations.
     */
    private static class WeightedSkewnessUdafImpl
            implements Udaf<Pair<Double, Double>, Struct, Double> {

        /**
         * Initializes the aggregation state with zeroed values.
         *
         * @return an empty {@link Struct} for tracking aggregation
         */
        @Override
        public Struct initialize() {
            return new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 0.0)
                .put(SUM_WEIGHTS, 0.0)
                .put(SUM_WEIGHT_SQUARES, 0.0)
                .put(SUM_WEIGHT_CUBES, 0.0);
        }

        /**
         * Aggregates a new (value, weight) pair into the current aggregation state.
         *
         * @param newValue the pair of input value and weight
         * @param aggregateValue the current aggregation state
         * @return a new {@link Struct} with updated sums
         */
        @Override
        public Struct aggregate(Pair<Double, Double> newValue, Struct aggregateValue) {
            // Extracting values from the Pair and the current state of the accumulator (Struct)
            double value = newValue.getLeft();
            double weight = newValue.getRight();

            // Extracting the current values from the Struct
            double sumValues = aggregateValue.getFloat64(SUM_VALUES);
            double sumWeights = aggregateValue.getFloat64(SUM_WEIGHTS);
            double sumWeightSquares = aggregateValue.getFloat64(SUM_WEIGHT_SQUARES);
            double sumWeightCubes = aggregateValue.getFloat64(SUM_WEIGHT_CUBES);

            // Weighted calculations
            sumValues += value * weight;
            sumWeights += weight;
            sumWeightSquares += weight * Math.pow(value, 2);
            sumWeightCubes += weight * Math.pow(value, 3);

            // Returning a new Struct with updated sums
            return new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, sumValues)
                .put(SUM_WEIGHTS, sumWeights)
                .put(SUM_WEIGHT_SQUARES, sumWeightSquares)
                .put(SUM_WEIGHT_CUBES, sumWeightCubes);
        }

        /**
         * Computes the final weighted skewness from the aggregated values.
         *
         * @param aggregate the aggregation struct
         * @return the weighted skewness, or 0.0 if weights or variance is zero
         */
        @Override
        public Double map(Struct aggregate) {
            // If no data was aggregated, return 0.0 as the skewness
            double sumValues = aggregate.getFloat64(SUM_VALUES);
            double sumWeights = aggregate.getFloat64(SUM_WEIGHTS);
            double sumWeightSquares = aggregate.getFloat64(SUM_WEIGHT_SQUARES);
            double sumWeightCubes = aggregate.getFloat64(SUM_WEIGHT_CUBES);

            // If sumWeights is 0, avoid division by zero
            if (sumWeights == 0.0) {
                return 0.0;
            }

            // Calculating the weighted mean
            double mean = sumValues / sumWeights;

            // Calculating the weighted variance
            double variance = (sumWeightSquares / sumWeights) - Math.pow(mean, 2);

            // Calculating the weighted skewness
            double skewness = (sumWeightCubes / sumWeights) - 3 * mean * (sumWeightSquares / sumWeights) + 2*Math.pow(mean, 3);

            // if variance is 0, avoid division by zero
            if (variance == 0.0) {
                return 0.0;
            }

            // Returning the weighted skewness
            return skewness / Math.pow(Math.max(variance, 0.0), 1.5);  // Normalize by the variance's 3/2 power
        }

        /**
         * Merges two intermediate aggregation structs.
         *
         * @param aggOne the first aggregation state
         * @param aggTwo the second aggregation state
         * @return a new merged {@link Struct}
         */
        @Override
        public Struct merge(Struct aggOne, Struct aggTwo) {
            // Merging two accumulators by summing their respective values
            double sumValues = aggOne.getFloat64(SUM_VALUES) + aggTwo.getFloat64(SUM_VALUES);
            double sumWeights = aggOne.getFloat64(SUM_WEIGHTS) + aggTwo.getFloat64(SUM_WEIGHTS);
            double sumWeightSquares = aggOne.getFloat64(SUM_WEIGHT_SQUARES) + aggTwo.getFloat64(SUM_WEIGHT_SQUARES);
            double sumWeightCubes = aggOne.getFloat64(SUM_WEIGHT_CUBES) + aggTwo.getFloat64(SUM_WEIGHT_CUBES);

            // Returning a new Struct with the merged sums
            return new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, sumValues)
                .put(SUM_WEIGHTS, sumWeights)
                .put(SUM_WEIGHT_SQUARES, sumWeightSquares)
                .put(SUM_WEIGHT_CUBES, sumWeightCubes);
        }
    }
}