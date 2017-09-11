package stroom.stats.xml;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.schema.v3.Statistics;
import stroom.stats.test.StatisticsHelper;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestStatisticsMarshaller {
    private final String RESOURCES_DIR = "src/test/resources";
    private final String PACKAGE_NAME = "/stroom/stats/xml/";
    private final String EXAMPLE_XML_01 = RESOURCES_DIR + PACKAGE_NAME + "statistics_01_threeGood.mxl";

    private final StatisticsMarshaller statisticsMarshaller;

    private final String statName = "MyStat";
    private final String statUuid = UUID.randomUUID().toString();
    private final String tag1 = "tag1";
    private final String tag2 = "tag2";
    private final String tag1val1 = tag1 + "val1";
    private final String tag2val1 = tag2 + "val1";
    private final ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);

    public TestStatisticsMarshaller() {
        this.statisticsMarshaller = new StatisticsMarshaller();
    }

    @Test
    public void unMarshallXml() throws Exception {


    }

    @Test
    public void testMarshllUnmarshall() throws Exception {

        Statistics.Statistic statistic = StatisticsHelper.buildCountStatistic(
                statUuid,
                statName,
                time,
                10L,
                StatisticsHelper.buildTagType(tag1, tag1val1),
                StatisticsHelper.buildTagType(tag2, tag2val1)
        );
        Statistics statistics = StatisticsHelper.buildStatistics(statistic);

        String xml = statisticsMarshaller.marshallToXml(statistics);

        Statistics statistics2 = statisticsMarshaller.unMarshallFromXml(xml);

        //TODO add in asserts

    }

}