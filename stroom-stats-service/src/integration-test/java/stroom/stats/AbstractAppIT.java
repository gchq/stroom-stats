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

package stroom.stats;

import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.testing.junit.DropwizardAppRule;
import io.dropwizard.util.Duration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.service.startup.App;
import stroom.stats.service.config.Config;

import javax.ws.rs.client.Client;

public abstract class AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAppIT.class);

    private static Client client;
    private static App app;

    protected static String STATISTICS_URL;
    protected static String QUERY_URL;

    protected static String BASE_TASKS_URL;
    protected static String HEALTH_CHECKS_URL;
    protected static String START_PROCESSING_URL;
    protected static String STOP_PROCESSING_URL;
    protected static int APPLICATION_PORT;
    protected static int ADMIN_PORT;

    //TODO this may prevent parallel execution of test classes
    @BeforeClass
    public static void setupClass() {
        // We need to enable typing otherwise abstract types, e.g. ExpressionItem, won't be deserialisable.
        RULE.getEnvironment().getObjectMapper().enableDefaultTyping();
        app = RULE.getApplication();
        JerseyClientConfiguration clientConfiguration = new JerseyClientConfiguration();
        clientConfiguration.setConnectionRequestTimeout(io.dropwizard.util.Duration.seconds(10));
        clientConfiguration.setConnectionTimeout(io.dropwizard.util.Duration.seconds(10));
        clientConfiguration.setTimeout(Duration.seconds(10));
        client = new JerseyClientBuilder(RULE.getEnvironment()).using(clientConfiguration).build("test client");
        APPLICATION_PORT = RULE.getLocalPort();
        ADMIN_PORT = RULE.getAdminPort();

        STATISTICS_URL = String.format("http://localhost:%d/statistics", APPLICATION_PORT);
        QUERY_URL = String.format("http://localhost:%d/search", APPLICATION_PORT);

        BASE_TASKS_URL = String.format("http://localhost:%d/admin/tasks/", ADMIN_PORT);
        HEALTH_CHECKS_URL = String.format("http://localhost:%d/admin/healthcheck?pretty=true", ADMIN_PORT);
        START_PROCESSING_URL = BASE_TASKS_URL + "startProcessing";
        STOP_PROCESSING_URL = BASE_TASKS_URL + "stopProcessing";

        LOGGER.info("Querl url:            {}", QUERY_URL);
        LOGGER.info("Health checks url:    {}", HEALTH_CHECKS_URL);
        LOGGER.info("Start processing url: {}, e.g. curl -X POST {}", START_PROCESSING_URL, START_PROCESSING_URL);
        LOGGER.info("Stop processing url:  {}, e.g. curl -X POST {}", STOP_PROCESSING_URL, STOP_PROCESSING_URL);
    }

    @ClassRule
    public static final DropwizardAppRule<Config> RULE = new DropwizardAppRule<>(App.class, "config.yml");


    protected static Client getClient() {
        return client;
    }

    public static DropwizardAppRule<Config> getAppRule() {
        return RULE;
    }

    public static App getApp() {
        return app;
    }

    protected StatsApiClient req() {
        return new StatsApiClient().client(getClient());
    }

}
