package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * A user-defined aggregate function (UDAF) for computing kurtosis in ksqlDB.
 *
 * This UDAF accepts a stream of double values and maintains a running calculation
 * of count, sum, squared, cubed, and fourth power sums to compute kurtosis.
 *
 * Registered under the function name {@code kurtosis}.
 *
 * This class is stateless and delegates all UDAF logic to its internal
 * {@link KurtosisUdafImpl} implementation class.
 */
@UdafDescription(
    name = "kurtosis",
    author = "Konstantin Charkseliani",
    version = "local-dev",
    description = "Calculates kurtosis of a series of values"
)
public class KurtosisUdaf {

    /** Field name for the internal aggregation state of the number of observations. */
    private static final String COUNT = "COUNT";

    /** Field name for the internal aggregation state of the sum of values. */
    private static final String SUM = "SUM";

    /** Field name for the internal aggregation state of the sum of squared values. */
    private static final String SUM_SQUARES = "SUM_SQUARES";

    /** Field name for the internal aggregation state of the sum of cubed values. */
    private static final String SUM_CUBES = "SUM_CUBES";

    /** Field name for the internal aggregation state of the sum of quartic powers of values. */
    private static final String SUM_QUARTIC = "SUM_QUARTIC";

    /** Schema used to represent the aggregation state. */
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().optional()
        .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
        .field(SUM, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_SQUARES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_CUBES, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(SUM_QUARTIC, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    /** Private constructor to prevent instantiation. */
    private KurtosisUdaf() {

    }

    /**
     * Factory method to register the kurtosis UDAF with ksqlDB (population version).
     *
     * @return a new instance of the UDAF implementation
     */
    @UdafFactory(
        description = "Calculates kurtosis of a series of values",
        aggregateSchema = "STRUCT<COUNT bigint, SUM double, SUM_SQUARES double, SUM_CUBES double, SUM_QUARTIC double>"
    )
    public static Udaf<Double, Struct, Double> createUdaf() {
        return new KurtosisUdafImpl(false);
    }

    /**
     * Factory method to register the kurtosis UDAF with optional bias correction for sample statistics.
     *
     * @param isSample whether to compute sample kurtosis (bias-corrected)
     * @return the UDAF implementation instance
     */
    @UdafFactory(
        description = "Calculates kurtosis of a series of values with optional bias correction",
        aggregateSchema = "STRUCT<COUNT bigint, SUM double, SUM_SQUARES double, SUM_CUBES double, SUM_QUARTIC double>"
    )
    public static Udaf<Double, Struct, Double> createUdaf(boolean isSample) {
        return new KurtosisUdafImpl(isSample);
    }

    /**
     * Implementation of the kurtosis UDAF logic.
     * 
     * Aggregates values into intermediate state and computes final kurtosis
     * using the fourth standardized moment formula.
     */
    private static class KurtosisUdafImpl implements Udaf<Double, Struct, Double> {

        /** Flag determining if population or sample (with bias correction) kurtosis is computed */
        private final boolean isSample;

        /**
         * Constructs an instance of the Kurtosis UDAF implementation.
         *
         * @param isSample whether to compute sample kurtosis (bias-corrected); 
         *                 must have at least 4 observations
         */
        public KurtosisUdafImpl(boolean isSample) {
            this.isSample = isSample;
        }

        /**
         * Initializes the aggregation state with zeroed values.
         *
         * @return an empty {@link Struct} for tracking aggregation
         */
        @Override
        public Struct initialize() {
            return new Struct(STRUCT_SCHEMA)
                .put(COUNT, 0L)
                .put(SUM, 0.0)
                .put(SUM_SQUARES, 0.0)
                .put(SUM_CUBES, 0.0)
                .put(SUM_QUARTIC, 0.0);
        }

        /**
         * Aggregates a new input value into the current aggregation state.
         *
         * @param newValue the new input value
         * @param aggregateValue the current aggregation state
         * @return an updated {@link Struct} with recalculated totals
         */
        @Override
        public Struct aggregate(Double newValue, Struct aggregateValue) {
            long count = aggregateValue.getInt64(COUNT);
            double sum = aggregateValue.getFloat64(SUM);
            double sumSquares = aggregateValue.getFloat64(SUM_SQUARES);
            double sumCubes = aggregateValue.getFloat64(SUM_CUBES);
            double sumQuartic = aggregateValue.getFloat64(SUM_QUARTIC);

            count += 1;
            sum += newValue;
            sumSquares += newValue * newValue;
            sumCubes += Math.pow(newValue, 3);
            sumQuartic += Math.pow(newValue, 4);

            return new Struct(STRUCT_SCHEMA)
                .put(COUNT, count)
                .put(SUM, sum)
                .put(SUM_SQUARES, sumSquares)
                .put(SUM_CUBES, sumCubes)
                .put(SUM_QUARTIC, sumQuartic);
        }

        /**
         * Computes the final kurtosis value from the aggregated state.
         *
         * @param aggregate the aggregation state
         * @return the kurtosis, 0 if count or variance is zero, NaN for sample kurtosis and count < 4
         */
        @Override
        public Double map(Struct aggregate) {

            long count = aggregate.getInt64(COUNT);

            if (count == 0) {
                return 0.0;
            }
            // At least 4 data points required to calculate sample kurtosis
            else if (isSample && count < 4) {
                return Double.NaN;
            }

            double mean = aggregate.getFloat64(SUM) / count;
            double variance = (aggregate.getFloat64(SUM_SQUARES) / count) - mean * mean;
            double m4 = (aggregate.getFloat64(SUM_QUARTIC) / count)
                        - 4 * mean * (aggregate.getFloat64(SUM_CUBES) / count)
                        + 6 * mean * mean * (aggregate.getFloat64(SUM_SQUARES) / count)
                        - 3 * Math.pow(mean, 4);

            if (isSample) {
                variance *= (count / (count - 1.0));
            }

            if (variance == 0.0) {
                return 0.0;
            }

            double kurtosis = m4 / (variance * variance);

            if (isSample) {
                // Apply sample correction factor
                kurtosis = ((count * count * (count + 1.0)) / ((count - 1.0) 
                    * (count - 2.0) * (count - 3.0))) * kurtosis
                    - (3.0 * (count - 1.0) * (count - 1.0)) 
                    / ((count - 2.0) * (count - 3.0));
            }

            return kurtosis;
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
                .put(COUNT, aggOne.getInt64(COUNT) + aggTwo.getInt64(COUNT))
                .put(SUM, aggOne.getFloat64(SUM) + aggTwo.getFloat64(SUM))
                .put(SUM_SQUARES, aggOne.getFloat64(SUM_SQUARES) + aggTwo.getFloat64(SUM_SQUARES))
                .put(SUM_CUBES, aggOne.getFloat64(SUM_CUBES) + aggTwo.getFloat64(SUM_CUBES))
                .put(SUM_QUARTIC, aggOne.getFloat64(SUM_QUARTIC) + aggTwo.getFloat64(SUM_QUARTIC));
        }
    }
}
