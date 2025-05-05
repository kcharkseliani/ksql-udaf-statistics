package com.kcharkseliani.kafka.ksql.statistics.util;

import io.confluent.ksql.function.udaf.UdafDescription;
import org.reflections.Reflections;
import java.util.Set;
import java.util.stream.Collectors;

public class UdafMetadata {
    public static Set<String> getDeclaredUdafNames() {
        Reflections reflections = new Reflections("com.kcharkseliani.kafka.ksql.statistics");

        return reflections.getTypesAnnotatedWith(UdafDescription.class).stream()
                .map(clazz -> clazz.getAnnotation(UdafDescription.class))
                .map(desc -> desc.name().toLowerCase())
                .collect(Collectors.toSet());
    }
}