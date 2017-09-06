/*
 * Copyright 2017 Crown Copyright
 *
 * This file is part of Stroom-Stats.
 *
 * Stroom-Stats is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stroom-Stats is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stroom-Stats.  If not, see <http://www.gnu.org/licenses/>.
 */

package stroom.stats.util.healthchecks;

import com.codahale.metrics.health.HealthCheck;
import javaslang.Tuple2;
import stroom.stats.util.HasName;

import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public interface HasHealthCheck extends HasName {

    /**
     * @return A non-null {@link com.codahale.metrics.health.HealthCheck.Result} object.
     */
    HealthCheck.Result getHealth();

    /**
     * @return An instance of {@link HealthCheck} that will provide
     * a {@link com.codahale.metrics.health.HealthCheck.Result}
     */
    default HealthCheck getHealthCheck() {
        return new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return getHealth();
            }
        };
    }

    static HasHealthCheck getAggregateHealthCheck(final String healthCheckName,
                                                  final List<HasHealthCheck> healthCheckProviders) {

        return new HasHealthCheck() {

            @Override
            public String getName() {
                return healthCheckName;
            }

            @Override
            public HealthCheck.Result getHealth() {
                List<Tuple2<String, HealthCheck.Result>> results = healthCheckProviders.stream()
                        .map(healthCheckProvider ->
                                new Tuple2<>(healthCheckProvider.getName(), healthCheckProvider.getHealth()))
                        .sorted(Comparator.comparing(Tuple2::_1))
                        .collect(Collectors.toList());

                boolean isHealthyOverall = !results.stream()
                        .anyMatch(result -> !result._2().isHealthy());

                HealthCheck.ResultBuilder builder = HealthCheck.Result.builder();

                if (isHealthyOverall) {
                    builder.healthy();
                } else {
                    builder.unhealthy();
                }
                builder.withMessage("Aggregated health check");
                results.forEach(result ->
                        builder.withDetail(result._1(), result._2()));

                HealthCheck.Result result = builder.build();
                return result;
            }
        };
    }

    /**
     * Return a Collector that collects to a TreeMap using the supplied key and value mappers. Duplicate keys will
     * result in a {@link RuntimeException}. Useful for creating sorted maps to go into HealthCheck detail values
     */
    static <T, K, U> Collector<T, ?, TreeMap<K, U>> buildTreeMapCollector(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends U> valueMapper) {

        return Collectors.toMap(
                keyMapper,
                valueMapper,
                (v1, v2) -> {
                    throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));
                },
                TreeMap::new);
    }
}
