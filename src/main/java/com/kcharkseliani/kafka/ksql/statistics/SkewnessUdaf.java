package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * A user-defined aggregate function (UDAF) for computing skewness in ksqlDB.
 *
 * This UDAF accepts a stream of double values and maintains a running calculation
 * of count, sum, sum of squares, and sum of cubes in order to compute skewness.
 *
 * Registered under the function name {@code skewness}.
 *
 * This class is stateless and delegates all UDAF logic to its internal
 * {@link SkewnessUdafImpl} implementation class.
 */
@UdafDescription(
    name = "skewness",
    author = "Konstantin Charkseliani",
    version = "0.1.0",
    description = "Calculates skewness of a series of values"
)
public class SkewnessUdaf {

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

    /** Private constructor to prevent instantiation. */
    private SkewnessUdaf() {

    }

    /**
     * Factory method to register the skewness UDAF with ksqlDB.
     *
     * @return a new instance of the UDAF implementation
     */
    @UdafFactory(
        description = "Calculates skewness of a series of values",
        aggregateSchema = "STRUCT<COUNT bigint, SUM double, SUM_SQUARES double, SUM_CUBES double>"
    )
    public static Udaf<Double, Struct, Double> createUdaf() {
        return new SkewnessUdafImpl(false);
    }

    @UdafFactory(
        description = "Calculates skewness of a series of values with optional bias correction",
        aggregateSchema = "STRUCT<COUNT bigint, SUM double, SUM_SQUARES double, SUM_CUBES double>"
    )
    public static Udaf<Double, Struct, Double> createUdaf(boolean isSample) {
        return new SkewnessUdafImpl(isSample);
    }

    /**
     * Implementation of the skewness UDAF logic.
     *
     * Aggregates values into intermediate state and computes final skewness
     * using the third standardized moment formula.
     */
    private static class SkewnessUdafImpl implements Udaf<Double, Struct, Double> {

        private final boolean isSample;

        public SkewnessUdafImpl(boolean isSample) {
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
                .put(SUM_CUBES, 0.0);
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

            count += 1;
            sum += newValue;
            sumSquares += newValue * newValue;
            sumCubes += newValue * newValue * newValue;

            return new Struct(STRUCT_SCHEMA)
                .put(COUNT, count)
                .put(SUM, sum)
                .put(SUM_SQUARES, sumSquares)
                .put(SUM_CUBES, sumCubes);
        }

        /**
         * Computes the final skewness value from the aggregated state.
         *
         * @param aggregate the aggregation state
         * @return the skewness, or 0 if count or variance is zero
         */
        @Override
        public Double map(Struct aggregate) {

            long count = aggregate.getInt64(COUNT);

            if (count == 0) {
                return 0.0;
            }

            double mean = aggregate.getFloat64(SUM) / count;
            double variance = (aggregate.getFloat64(SUM_SQUARES) / count) - Math.pow(mean, 2);

            if (variance == 0.0) {
                return 0.0;
            }

            double m3 = (aggregate.getFloat64(SUM_CUBES) / count)
                        - 3 * mean * (aggregate.getFloat64(SUM_SQUARES) / count)
                        + 2 * Math.pow(mean, 3);

            double skewness = m3 / Math.pow(Math.max(variance, 0.0), 1.5);

            if (isSample) {
                // Apply sample correction factor
                skewness *= (count * count) / ((count - 1) * (count - 2));
            }

            return skewness;
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
                .put(SUM_CUBES, aggOne.getFloat64(SUM_CUBES) + aggTwo.getFloat64(SUM_CUBES));
        }
    }
}