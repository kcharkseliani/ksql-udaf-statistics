// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Konstantin Charkseliani

package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * A user-defined aggregate function (UDAF) for computing weighted standard deviation in ksqlDB.
 * 
 * This UDAF processes pairs of (value, weight) and maintains running sums required to compute
 * the standard deviation in a weighted manner.
 * 
 * Registered under the function name {@code stddev_weighted}.
 * 
 * This class is stateless and delegates all UDAF logic to its internal 
 * {@link WeightedStdDevUdafImpl} implementation class.
 */
@UdafDescription(
    name = "stddev_weighted",
    author = "Konstantin Charkseliani",
    version = "local-dev",
    description = "Calculates standard deviation based on weights of each value"
)
public class WeightedStdDevUdaf {

    /** Field name for the internal aggregation state of the sum of values. */
    private static final String SUM_VALUES = "SUM_VALUES";

    /** Field name for the internal aggregation state of the sum of weights. */
    private static final String SUM_WEIGHTS = "SUM_WEIGHTS";

    /** Field name for the internal aggregation state of the sum of weighted squares. */
    private static final String SUM_WEIGHT_SQUARES = "SUM_WEIGHT_SQUARES";

    /** Schema used to structure the aggregation result. */
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
        .field(SUM_VALUES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHTS, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHT_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    /** Private constructor to prevent instantiation. */
    private WeightedStdDevUdaf() {
    }

    /**
     * Factory method to register the weighted standard deviation UDAF with ksqlDB.
     *
     * @return a new instance of the UDAF implementation
     */
    @UdafFactory(description = "Calculates the weighted standard deviation of a stream of values with weights.",
            aggregateSchema = "STRUCT<SUM_VALUES double, SUM_WEIGHTS double, SUM_WEIGHT_SQUARES double>")
    public static Udaf<Pair<Double, Double>, Struct, Double> createUdaf() {
        return new WeightedStdDevUdafImpl();
    }

    /**
     * Implementation of the weighted standard deviation UDAF logic.
     * 
     * Aggregates a stream of (value, weight) pairs into a struct and calculates
     * the final result in the {@code map} method. Also supports merging of partial results.
     */
    private static class WeightedStdDevUdafImpl
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
                .put(SUM_WEIGHT_SQUARES, 0.0);
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

            // Updating the sums
            sumValues += value * weight;
            sumWeights += weight;
            sumWeightSquares += weight * Math.pow(value, 2);

            // Returning a new Struct with updated sums
            return new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, sumValues)
                .put(SUM_WEIGHTS, sumWeights)
                .put(SUM_WEIGHT_SQUARES, sumWeightSquares);
        }

        /**
         * Computes the final weighted standard deviation from the aggregated values.
         *
         * @param aggregate the aggregation struct
         * @return the weighted standard deviation, or 0.0 if weights are zero
         */
        @Override
        public Double map(Struct aggregate) {
            // If no data was aggregated, return 0.0 as the standard deviation
            double sumValues = aggregate.getFloat64(SUM_VALUES);
            double sumWeights = aggregate.getFloat64(SUM_WEIGHTS);
            double sumWeightSquares = aggregate.getFloat64(SUM_WEIGHT_SQUARES);

            // If sumWeights is 0, avoid division by zero
            if (sumWeights == 0.0) {
                return 0.0;
            }

            // Calculating the weighted mean
            double mean = sumValues / sumWeights;

            // Calculating the weighted variance
            double variance = (sumWeightSquares / sumWeights) - Math.pow(mean, 2);

            // Returning the weighted standard deviation (square root of the variance)
            return Math.sqrt(Math.max(variance, 0.0)); // Ensure non-negative variance
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

            // Returning a new Struct with the merged sums
            return new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, sumValues)
                .put(SUM_WEIGHTS, sumWeights)
                .put(SUM_WEIGHT_SQUARES, sumWeightSquares);
        }
    }
}