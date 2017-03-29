package stroom.stats.testdata;

import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.schema.Statistics;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DummyStat {
    private List<String> tags = new ArrayList<>();
    private StatisticConfigurationEntity statisticConfigurationEntity;
    private Statistics statistics;
    private ZonedDateTime now;

    public DummyStat tags(String... tags){
        this.tags.addAll(Arrays.asList(tags));
        return this;
    }

    public DummyStat statisticConfigurationEntity(StatisticConfigurationEntity statisticConfigurationEntity){
        this.statisticConfigurationEntity = statisticConfigurationEntity;
        return this;
    }

    public DummyStat statistics(Statistics statistics){
        this.statistics = statistics;
        return this;
    }

    public StatisticConfigurationEntity statisticConfigurationEntity() {
        return statisticConfigurationEntity;
    }

    public Statistics statistics() {
        return statistics;
    }

    public DummyStat time(ZonedDateTime now) {
        this.now = now;
        return this;
    }

    public ZonedDateTime time() {
        return now;
    }
}
