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

package stroom.stats.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.streams.StatisticsIngestService;

import javax.inject.Inject;
import java.io.PrintWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Starts
 */
@SuppressWarnings("unused") //exposed as admin endpoint by dropwizard
public class StartProcessingTask extends Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartProcessingTask.class);

    public static final String TASK_NAME = "startProcessing";

    private final StatisticsIngestService statisticsIngestService;

    @Inject
    public StartProcessingTask(final StatisticsIngestService statisticsIngestService) {
        super(TASK_NAME);
        this.statisticsIngestService = statisticsIngestService;
    }

    @Override
    public void execute(final ImmutableMultimap<String, String> parameters, final PrintWriter output) throws Exception {

        LOGGER.info("{} endpoint called", TASK_NAME);

        @SuppressWarnings("FutureReturnValueIgnored")
        Future<Void> future = CompletableFuture
                .runAsync(statisticsIngestService::start)
                .handle((aVoid, ex) -> {
                    if (ex != null) {
                        LOGGER.error("Task {} failed with error {}", TASK_NAME, ex.getMessage(), ex);
                    } else {
                        LOGGER.info("Task {} completed successfully", TASK_NAME);
                    }
                    return aVoid;
                });
    }
}
