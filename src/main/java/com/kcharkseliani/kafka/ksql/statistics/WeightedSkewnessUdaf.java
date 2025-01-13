package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "skewness_weighted",
                 author = "Konstantin Charkseliani",
                 version = "0.1.0",
                 description = "Calculates skewness based on weights of each value")

public class WeightedSkewnessUdaf {

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

    private WeightedSkewnessUdaf() {
    }

    @UdafFactory(description = "Calculates the weighted skewness of a stream of values with weights.",
            aggregateSchema = "STRUCT<SUM_VALUES double, SUM_WEIGHTS double, SUM_WEIGHT_SQUARES double, SUM_WEIGHT_CUBES double>")
    public static Udaf<Pair<Double, Double>, Struct, Double> createUdaf() {
        return new WeightedSkewnessUdafImpl();
    }

    private static class WeightedSkewnessUdafImpl
            implements Udaf<Pair<Double, Double>, Struct, Double> {

        @Override
        public Struct initialize() {
            return new Struct(STRUCT_SCHEMA)
                .put(SUM_VALUES, 0.0)
                .put(SUM_WEIGHTS, 0.0)
                .put(SUM_WEIGHT_SQUARES, 0.0)
                .put(SUM_WEIGHT_CUBES, 0.0);
        }

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

            // Returning the weighted skewness
            return skewness / Math.pow(Math.max(variance, 0.0), 1.5);  // Normalize by the variance's 3/2 power
        }

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
