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

import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.dashboard.expression.FieldIndexMap;
import stroom.query.Coprocessor;
import stroom.query.CoprocessorSettings;
import stroom.query.CoprocessorSettingsMap;
import stroom.query.Payload;
import stroom.query.SearchResponseCreator;
import stroom.query.TableCoprocessor;
import stroom.query.TableCoprocessorSettings;
import stroom.query.api.DocRef;
import stroom.query.api.FlatResult;
import stroom.query.api.OffsetRange;
import stroom.query.api.Param;
import stroom.query.api.Result;
import stroom.query.api.ResultRequest;
import stroom.query.api.Row;
import stroom.query.api.SearchRequest;
import stroom.query.api.SearchResponse;
import stroom.query.api.TableResult;
import stroom.stats.adapters.StatisticEventAdapter;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.StatisticDataPoint;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.schema.Statistics;
import stroom.util.shared.HasTerminate;
import stroom.util.task.TaskMonitor;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

//TODO everything about this class needs work, including its name
//TODO Does this need to be a singleton?
//@Singleton
public class HBaseClient implements Managed {
    private final Logger LOGGER = LoggerFactory.getLogger(HBaseClient.class);

    private final StatisticsService statisticsService;
    private final StatisticConfigurationService statisticConfigurationService;

    @Inject
    public HBaseClient(final StatisticsService statisticsService, final StatisticConfigurationService statisticConfigurationService) {
        this.statisticsService = statisticsService;
        this.statisticConfigurationService = statisticConfigurationService;
    }

    public void addStatistics(Statistics statistics) {
        Preconditions.checkNotNull(statistics);

        // TODO Change putEvents to handle statistics
        //TODO maybe this should talk direct to EventStores
        //TODO need to figure out how much of the code in AbstractStatisticsService and
        // HBaseStatisticsService needs to live in Stroom-stats and how much in stroom

        //TODO not very efficient to have to do this conversion but for the moment
        //it gets things working
        List<StatisticEvent> statisticEvents = statistics.getStatistic().stream()
                .map(StatisticEventAdapter::convert)
                .collect(Collectors.toList());

        statisticsService.putEvents(statisticEvents);
    }


    public SearchResponse query(SearchRequest searchRequest) {

        DocRef statisticStoreRef = searchRequest.getQuery().getDataSource();
        //TODO Need to consider how to handle an unknown docref
//        final Try<StatisticConfiguration> optStatisticConfiguration =
        return statisticConfigurationService.fetchStatisticConfigurationByUuid(statisticStoreRef.getUuid())
                .map(statisticConfiguration -> {


                    // TODO: possibly the mapping from the componentId to the coprocessorsettings map is a bit odd.
                    final CoprocessorSettingsMap coprocessorSettingsMap = CoprocessorSettingsMap.create(searchRequest);

                    Map<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> coprocessorMap = new HashMap<>();
                    // TODO: Mapping to this is complicated! it'd be nice not to have to do this.
                    final FieldIndexMap fieldIndexMap = new FieldIndexMap(true);

                    // Compile all of the result component options to optimise pattern matching etc.
                    if (coprocessorSettingsMap.getMap() != null) {
                        for (final Map.Entry<CoprocessorSettingsMap.CoprocessorKey, CoprocessorSettings> entry : coprocessorSettingsMap.getMap().entrySet()) {
                            final CoprocessorSettingsMap.CoprocessorKey coprocessorId = entry.getKey();
                            final CoprocessorSettings coprocessorSettings = entry.getValue();

                            // Create a parameter map.
                            final Map<String, String> paramMap = Collections.emptyMap();
                            if (searchRequest.getQuery().getParams() != null) {
                                for (final Param param : searchRequest.getQuery().getParams()) {
                                    paramMap.put(param.getKey(), param.getValue());
                                }
                            }

                            final Coprocessor coprocessor = createCoprocessor(
                                    coprocessorSettings, fieldIndexMap, paramMap, new HasTerminate(){
                                        //TODO do something about this
                                        @Override
                                        public void terminate() {
                                            System.out.println("terminating");
                                        }

                                        @Override
                                        public boolean isTerminated() {
                                            return false;
                                        }
                                    });

                            if (coprocessor != null) {
                                coprocessorMap.put(coprocessorId, coprocessor);
                            }
                        }
                    }

                    StatisticDataSet statisticDataSet = statisticsService.searchStatisticsData(searchRequest.getQuery(), statisticConfiguration);
                    SearchResponse.Builder searchResponseBuilder = new SearchResponse.Builder(true);

                    //TODO TableCoprocessor is doing a lot of work to pre-process and aggregate the datas


                    for(StatisticDataPoint statisticDataPoint : statisticDataSet){
                        String[] tags = new String[fieldIndexMap.size()];
                        statisticDataPoint.getTags().forEach(statisticTag -> {
                            int position = fieldIndexMap.get(statisticTag.getTag());
                            if(position != -1){
                                tags[position] = statisticTag.getValue();
                            }
                            //TODO what are the special names for the other values in the data point? Ask Andy!
                        });

                        coprocessorMap.entrySet().forEach(coprocessor -> {
                            coprocessor.getValue().receive(tags);
                        });
                    }



                    // TODO pyutting things into a payload and taking them out again is a waste of time in this case. We could use a queue instead and that'd be fine.
                    //TODO: 'Payload' is a cluster specific name - what lucene ships back from a node.
                    // Produce payloads for each coprocessor.
                    Map<CoprocessorSettingsMap.CoprocessorKey, Payload> payloadMap = null;
                    if (coprocessorMap != null && coprocessorMap.size() > 0) {
                        for (final Map.Entry<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> entry : coprocessorMap.entrySet()) {
                            final Payload payload = entry.getValue().createPayload();
                            if (payload != null) {
                                if (payloadMap == null) {
                                    payloadMap = new HashMap<>();
                                }

                                payloadMap.put(entry.getKey(), payload);
                            }
                        }
                    }


                    StatisticsStore store = new StatisticsStore();
                    store.process(coprocessorSettingsMap);
                    store.coprocessorMap(coprocessorMap);
                    store.payloadMap(payloadMap);

                    SearchResponseCreator searchResponseCreator = new SearchResponseCreator(store);
                    SearchResponse searchResponse = searchResponseCreator.create(searchRequest);

                    return searchResponse;
                })
                .orElseGet(() -> {
                    SearchResponse searchResponse = new SearchResponse(
                            Arrays.asList(),
                            Arrays.asList(),
                            Arrays.asList("Statistic configuration could not be found for uuid " + statisticStoreRef.getUuid()),
                            true);
                    return searchResponse;
                });
    }

    //TODO need an endpoint to kick off a purge for a list of docrefs

    public void purgeAllData(List<DocRef> docRefs) {
        //Quietly ignores docRefs that don't exist, may want to change this behaviour
        List<StatisticConfiguration> statisticConfigurations = docRefs.stream()
                .map(docRef -> statisticConfigurationService.fetchStatisticConfigurationByUuid(docRef.getUuid()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        statisticsService.purgeAllData(statisticConfigurations);
    }

    /**
     * Deletes ALL data from all granularities for all statistic configurations.
     * Use with EXTREME caution. Probably not for exposure to the web service
     */
    public void purgeAllData() {
        statisticsService.purgeAllData(statisticConfigurationService.fetchAll());
    }

    //TODO need an endpoint to kick off a retention period purge if we want to use cron jobs
    //to fire cluster level scheduled events



    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {
        statisticsService.shutdown();
    }

        //TODO Delete this when we have something more sensible to return.

    private SearchResponse getDummySearchResponse() {
        SearchResponse searchResponse = new SearchResponse(
                Arrays.asList("highlight1", "highlight2"),
                Arrays.asList(
                new TableResult(
                    "componentId",
                    new ArrayList<>(Arrays.asList(
                            new Row("groupKey", Arrays.asList("value1", "value2"), 5))),
                    new OffsetRange(1, 2),
                    1,
                    "tableResultError"
                )),
                Arrays.asList("error1", "error2"),
            false
        );

        return searchResponse;
    }




    //TODO This lives in stroom and should have a copy here
    public static Coprocessor createCoprocessor(final CoprocessorSettings settings,
                              final FieldIndexMap fieldIndexMap, final Map<String, String> paramMap, final HasTerminate taskMonitor) {
        if (settings instanceof TableCoprocessorSettings) {
            final TableCoprocessorSettings tableCoprocessorSettings = (TableCoprocessorSettings) settings;
            final TableCoprocessor tableCoprocessor = new TableCoprocessor(tableCoprocessorSettings,
                    fieldIndexMap, taskMonitor, paramMap);
            return tableCoprocessor;
        }
        return null;
    }

}
