

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

package stroom.stats.main;

import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.table.EventStoreColumnFamily;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.io.IOException;
import java.util.Arrays;

public class HbaseDataViewer extends AbstractAppRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseDataViewer.class);

    public static void main(String[] args) {
        new HbaseDataViewer();
    }

    @Override
    void run(final Injector injector) throws Exception {

        final StatisticsTestService statisticsTestService = injector.getInstance(StatisticsTestService.class);

//        EventStoreTimeIntervalEnum[] intervals = {EventStoreTimeIntervalEnum.DAY};
        EventStoreTimeIntervalEnum[] intervals = EventStoreTimeIntervalEnum.values();

//        EventStoreColumnFamily[] families = {EventStoreColumnFamily.VALUES};
        EventStoreColumnFamily[] families = EventStoreColumnFamily.values();
//
        // scan all rows in the hourly event store table and output the results.
        Arrays.stream(intervals).forEach(interval ->
                Arrays.stream(families).forEach(colFam -> {
                    try {
                        statisticsTestService.scanAllData(interval, colFam, 2000);
                    } catch (IOException e) {
                        throw new RuntimeException(String.format("Exception scanning all data for col fam %s and interval %s", colFam, interval), e);
                    }
                })
        );

    }
}
