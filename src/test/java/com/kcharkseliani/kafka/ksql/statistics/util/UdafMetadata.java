// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Konstantin Charkseliani

package com.kcharkseliani.kafka.ksql.statistics.util;

import io.confluent.ksql.function.udaf.UdafDescription;
import org.reflections.Reflections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for retrieving metadata about UDAFs (User-Defined Aggregate Functions)
 * defined in the project using the {@link UdafDescription} annotation.
 */
public class UdafMetadata {

    /**
     * Scans the project classpath for UDAF classes annotated with {@link UdafDescription}
     * and collects their declared names in lowercase.
     *
     * @return a set of UDAF names registered via {@code @UdafDescription}
     */
    public static Set<String> getDeclaredUdafNames() {
        Reflections reflections = new Reflections("com.kcharkseliani.kafka.ksql.statistics");

        return reflections.getTypesAnnotatedWith(UdafDescription.class).stream()
                // For each class, retrieve its @UdafDescription annotation instance
                .map(clazz -> clazz.getAnnotation(UdafDescription.class))
                .map(desc -> desc.name().toLowerCase())
                .collect(Collectors.toSet());
    }
}