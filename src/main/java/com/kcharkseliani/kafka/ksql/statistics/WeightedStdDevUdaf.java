package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.Pair;

import java.util.ArrayList;
import java.util.List;

@UdafDescription(name = "stddev_weighted",
                 author = "Konstantin Charkseliani",
                 version = "0.1.0",
                 description = "Calculates standard deviation based on weights of each value")
public class WeightedStdDevUdaf {

    private WeightedStdDevUdaf() {
    }

    @UdafFactory(description = "Calculates the weighted standard deviation of a stream of values with weights.")
    public static Udaf<Pair<Double, Double>, List<Pair<Double, Double>>, Double> createUdaf() {
        return new WeightedStdDevUdafImpl();
    }

    private static class WeightedStdDevUdafImpl 
            implements Udaf<Pair<Double, Double>, List<Pair<Double, Double>>, Double> {

        @Override
        public List<Pair<Double, Double>> initialize() {
            return new ArrayList<Pair<Double, Double>>();
        }

        @Override
        public List<Pair<Double, Double>> aggregate(
                Pair<Double, Double> newValue, List<Pair<Double, Double>> aggregateValue) {
            aggregateValue.add(newValue); // Add the new value-weight pair to the list
            return aggregateValue;
        }

        @Override
        public Double map(List<Pair<Double, Double>> intermediate) {
            if (intermediate.isEmpty()) {
                return 0.0; // Handle empty case
            }

            double sumValues = 0.0;
            double sumWeights = 0.0;
            double sumWeightSquares = 0.0;

            for (Pair<Double, Double> pair : intermediate) {
                double value = pair.getLeft();
                double weight = pair.getRight();

                sumValues += value * weight;
                sumWeights += weight;
                sumWeightSquares += weight * Math.pow(value, 2);
            }

            if (sumWeights == 0.0) {
                return 0.0; // Avoid division by zero
            }

            double mean = sumValues / sumWeights;
            double variance = (sumWeightSquares / sumWeights) - Math.pow(mean, 2);

            return Math.sqrt(Math.max(variance, 0.0)); // Ensure non-negative variance
        }

        @Override
        public List<Pair<Double, Double>> merge(
                List<Pair<Double, Double>> aggOne, List<Pair<Double, Double>> aggTwo) {
            aggOne.addAll(aggTwo); // Merge the two lists
            return aggOne;
        }
    }
}
