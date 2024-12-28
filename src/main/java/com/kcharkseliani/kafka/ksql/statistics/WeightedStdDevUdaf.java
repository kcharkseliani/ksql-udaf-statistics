package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import java.util.ArrayList;
import java.util.List;

@UdafDescription(name = "weighted_stddev",
                 author = "Konstantin Charkseliani",
                 version = "0.1.0",
                 description = "Calculates standard deviation based on weights of each value")
public class WeightedStdDevUdaf {

    private WeightedStdDevUdaf() {
    }

    @UdafFactory(description = "Calculates the weighted standard deviation of a stream of values with weights.")
    public static Udaf<WeightedValue, WeightedIntermediate, Double> createUdaf() {
        return new WeightedStdDevUdafImpl();
    }

    // Class to store value and weight
    public static class WeightedValue {
        public final double value;
        public final double weight;

        public WeightedValue(double value, double weight) {
            this.value = value;
            this.weight = weight;
        }
    }

    // Intermediate representation for aggregation
    public static class WeightedIntermediate {
        public double sumValues;      // Sum of weighted values
        public double sumWeights;     // Sum of weights
        public double sumWeightSquares; // Sum of weight * value^2

        public WeightedIntermediate() {
            this.sumValues = 0.0;
            this.sumWeights = 0.0;
            this.sumWeightSquares = 0.0;
        }
    }

    private static class WeightedStdDevUdafImpl implements Udaf<WeightedValue, WeightedIntermediate, Double> {

        @Override
        public WeightedIntermediate initialize() {
            return new WeightedIntermediate();
        }

        @Override
        public WeightedIntermediate aggregate(WeightedValue newValue, WeightedIntermediate aggregateValue) {
            aggregateValue.sumValues += newValue.value * newValue.weight;
            aggregateValue.sumWeights += newValue.weight;
            aggregateValue.sumWeightSquares += newValue.weight * Math.pow(newValue.value, 2);
            return aggregateValue;
        }

        @Override
        public Double map(WeightedIntermediate intermediate) {
            if (intermediate.sumWeights == 0.0) {
                return 0.0; // Avoid division by zero
            }
            double mean = intermediate.sumValues / intermediate.sumWeights;
            double variance = (intermediate.sumWeightSquares / intermediate.sumWeights) - Math.pow(mean, 2);
            return Math.sqrt(Math.max(variance, 0.0)); // Avoid negative due to floating-point errors
        }

        @Override
        public WeightedIntermediate merge(WeightedIntermediate aggOne, WeightedIntermediate aggTwo) {
            WeightedIntermediate merged = new WeightedIntermediate();
            merged.sumValues = aggOne.sumValues + aggTwo.sumValues;
            merged.sumWeights = aggOne.sumWeights + aggTwo.sumWeights;
            merged.sumWeightSquares = aggOne.sumWeightSquares + aggTwo.sumWeightSquares;
            return merged;
        }
    }
}
