package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * A user-defined aggregate function (UDAF) for computing weighted kurtosis in ksqlDB.
 *
 * This UDAF processes a stream of (value, weight) pairs and maintains running totals for
 * weighted moments to calculate the kurtosis, normalized by the square of the variance.
 *
 * Registered under the function name {@code kurtosis_weighted}.
 *
 * This class is stateless and delegates computation to its internal 
 * {@link WeightedKurtosisUdafImpl} implementation class.
 */
@UdafDescription(
    name = "kurtosis_weighted",
    author = "Konstantin Charkseliani",
    version = "0.1.0",
    description = "Calculates kurtosis of a stream of values with associated weights"
)
public class WeightedKurtosisUdaf {

    /** Field name for the internal aggregation state of the number of observations. */
    private static final String SUM_VALUES = "SUM_VALUES";

    /** Field name for the internal aggregation state of the sum of values. */
    private static final String SUM_WEIGHTS = "SUM_WEIGHTS";

    /** Field name for the internal aggregation state of the sum of squared values. */
    private static final String SUM_WEIGHT_SQUARES = "SUM_WEIGHT_SQUARES";

    /** Field name for the internal aggregation state of the sum of cubed values. */
    private static final String SUM_WEIGHT_CUBES = "SUM_WEIGHT_CUBES";

    /** Field name for the internal aggregation state of the sum of quartic powers of values. */
    private static final String SUM_WEIGHT_QUARTIC = "SUM_WEIGHT_QUARTIC";

    /** Schema used to structure the aggregation result. */
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
        .field(SUM_VALUES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHTS, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHT_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHT_CUBES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_WEIGHT_QUARTIC, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    /** Private constructor to prevent instantiation. */
    private WeightedKurtosisUdaf() {

    }

    /**
     * Factory method to register the weighted kurtosis UDAF with ksqlDB.
     *
     * @return a new instance of the UDAF implementation
     */
    @UdafFactory(
        description = "Calculates weighted kurtosis of a series of values with weights",
        aggregateSchema = "STRUCT<SUM_VALUES double, SUM_WEIGHTS double, " +
                          "SUM_WEIGHT_SQUARES double, SUM_WEIGHT_CUBES double, SUM_WEIGHT_QUARTIC double>")
    public static Udaf<Pair<Double, Double>, Struct, Double> createUdaf() {
        return new WeightedKurtosisUdafImpl();
    }

    /**
     * Implementation of the weighted kurtosis UDAF logic.
     * 
     * Aggregates a stream of (value, weight) pairs into a struct and calculates
     * weighted kurtosis during mapping or merges two aggregations.
     */
    private static class WeightedKurtosisUdafImpl 
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
                .put(SUM_WEIGHT_CUBES, 0.0)
                .put(SUM_WEIGHT_QUARTIC, 0.0);
        }

        /**
         * Aggregates a new (value, weight) pair into the current aggregation state.
         *
         * @param newValue the pair of input value and weight
         * @param aggregateValue the current aggregation state
         * @return an updated {@link Struct} with updated sums
         */
        @Override
        public Struct aggregate(Pair<Double, Double> newValue, Struct aggregateValue) {
            
            // Extracting values from the Pair and the current state of the accumulator (Struct)
            double value = newValue.getLeft();
            double weight = newValue.getRight();

            // Returning a new Struct with updated running sums
            return new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, aggregateValue.getFloat64(SUM_VALUES) + weight * value)
                .put(SUM_WEIGHTS, aggregateValue.getFloat64(SUM_WEIGHTS) + weight)
                .put(SUM_WEIGHT_SQUARES, aggregateValue
                    .getFloat64(SUM_WEIGHT_SQUARES) + weight * value * value)
                .put(SUM_WEIGHT_CUBES, aggregateValue
                    .getFloat64(SUM_WEIGHT_CUBES) + weight * Math.pow(value, 3))
                .put(SUM_WEIGHT_QUARTIC, aggregateValue
                    .getFloat64(SUM_WEIGHT_QUARTIC) + weight * Math.pow(value, 4));
        }

        /**
         * Computes the final weighted kurtosis from the aggregated values.
         *
         * @param aggregate the aggregation struct
         * @return the weighted kurtosis, or 0.0 if weights or variance is zero
         */
        @Override
        public Double map(Struct aggregate) {
            
            double sumW = aggregate.getFloat64(SUM_WEIGHTS);

            // If sumWeights is 0, avoid division by zero
            if (sumW == 0.0) {
                return 0.0;
            }

            // Calculating the weighted mean
            double mean = aggregate.getFloat64(SUM_VALUES) / sumW;

            // Calculating the weighted variance
            double m2 = (aggregate.getFloat64(SUM_WEIGHT_SQUARES) / sumW) - Math.pow(mean, 2);

            // if variance is 0, avoid division by zero
            if (m2 == 0.0) {
                return 0.0;
            }

            // Calculating the weighted kurtosis
            double m4 = (aggregate.getFloat64(SUM_WEIGHT_QUARTIC) / sumW)
                      - 4 * mean * (aggregate.getFloat64(SUM_WEIGHT_CUBES) / sumW)
                      + 6 * mean * mean * (aggregate.getFloat64(SUM_WEIGHT_SQUARES) / sumW)
                      - 3 * Math.pow(mean, 4);

            // Returning standardized weighted kurtosis
            return m4 / (m2 * m2);    
        }

        /**
         * Merges two aggregation structs by summing all corresponding fields.
         *
         * @param aggOne the first intermediate aggregation state
         * @param aggTwo the second intermediate aggregation state
         * @return a merged {@link Struct} combining both states
         */
        @Override
        public Struct merge(Struct aggOne, Struct aggTwo) {

            return new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, aggOne.getFloat64(SUM_VALUES) + aggTwo.getFloat64(SUM_VALUES))
                .put(SUM_WEIGHTS, aggOne.getFloat64(SUM_WEIGHTS) + aggTwo.getFloat64(SUM_WEIGHTS))
                .put(SUM_WEIGHT_SQUARES, aggOne.getFloat64(SUM_WEIGHT_SQUARES) 
                    + aggTwo.getFloat64(SUM_WEIGHT_SQUARES))
                .put(SUM_WEIGHT_CUBES, aggOne.getFloat64(SUM_WEIGHT_CUBES) 
                    + aggTwo.getFloat64(SUM_WEIGHT_CUBES))
                .put(SUM_WEIGHT_QUARTIC, aggOne.getFloat64(SUM_WEIGHT_QUARTIC) 
                    + aggTwo.getFloat64(SUM_WEIGHT_QUARTIC));
        }
    }
}