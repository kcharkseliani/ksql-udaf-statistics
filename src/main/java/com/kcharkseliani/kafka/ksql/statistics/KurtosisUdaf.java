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
    version = "0.1.0",
    description = "Calculates kurtosis of a series of values"
)
public class KurtosisUdaf {

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

    private KurtosisUdaf() {}

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
     */
    private static class KurtosisUdafImpl implements Udaf<Double, Struct, Double> {

        private final boolean isSample;

        /**
         * Constructs the kurtosis UDAF.
         *
         * @param isSample whether to compute sample kurtosis (bias-corrected); 
         * must have at least 4 observations
         */
        public KurtosisUdafImpl(boolean isSample) {
            this.isSample = isSample;
        }

        @Override
        public Struct initialize() {
            return new Struct(STRUCT_SCHEMA)
                .put(COUNT, 0L)
                .put(SUM, 0.0)
                .put(SUM_SQUARES, 0.0)
                .put(SUM_CUBES, 0.0)
                .put(SUM_QUARTIC, 0.0);
        }

        @Override
        public Struct aggregate(Double value, Struct aggregate) {
            long count = aggregate.getInt64(COUNT);
            double sum = aggregate.getFloat64(SUM);
            double sumSquares = aggregate.getFloat64(SUM_SQUARES);
            double sumCubes = aggregate.getFloat64(SUM_CUBES);
            double sumQuartic = aggregate.getFloat64(SUM_QUARTIC);

            count += 1;
            sum += value;
            sumSquares += value * value;
            sumCubes += Math.pow(value, 3);
            sumQuartic += Math.pow(value, 4);

            return new Struct(STRUCT_SCHEMA)
                .put(COUNT, count)
                .put(SUM, sum)
                .put(SUM_SQUARES, sumSquares)
                .put(SUM_CUBES, sumCubes)
                .put(SUM_QUARTIC, sumQuartic);
        }

        @Override
        public Double map(Struct aggregate) {
            return 0.0;
        }

        @Override
        public Struct merge(Struct a, Struct b) {
            return new Struct(STRUCT_SCHEMA)
                .put(COUNT, a.getInt64(COUNT) + b.getInt64(COUNT))
                .put(SUM, a.getFloat64(SUM) + b.getFloat64(SUM))
                .put(SUM_SQUARES, a.getFloat64(SUM_SQUARES) + b.getFloat64(SUM_SQUARES))
                .put(SUM_CUBES, a.getFloat64(SUM_CUBES) + b.getFloat64(SUM_CUBES))
                .put(SUM_QUARTIC, a.getFloat64(SUM_QUARTIC) + b.getFloat64(SUM_QUARTIC));
        }
    }
}
