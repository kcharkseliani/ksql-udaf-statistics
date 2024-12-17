package com.kcharkseliani.kafka.ksql.statistics;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

@UdafDescription(name = "weighted_stddev",
                 author = "Konstantin Charkseliani",
                 version = "0.1.0",
                 description = "Calculates standard deviation based on weights of each value")
public class WeightedStdDevUdaf {

}
