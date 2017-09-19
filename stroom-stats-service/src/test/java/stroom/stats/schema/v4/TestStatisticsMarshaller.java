package stroom.stats.schema.v4;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.test.StatisticsHelper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.function.Function;

public class TestStatisticsMarshaller {
    private final String RESOURCES_DIR = "src/test/resources";
    private final String PACKAGE_NAME = "/stroom/stats/schema/v3/";
    private final String EXAMPLE_XML_01 = RESOURCES_DIR + PACKAGE_NAME + "statistics_01_threeGood.xml";

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
    public void unMarshallFromXml() throws Exception {

        String xmlStr = new String(Files.readAllBytes(Paths.get(EXAMPLE_XML_01)), StandardCharsets.UTF_8);
        Statistics statistics = statisticsMarshaller.unMarshallFromXml(xmlStr);
        Assertions.assertThat(statistics.getStatistic()).hasSize(3);

        Statistics.Statistic statistic1 = statistics.getStatistic().get(0);

        Assertions.assertThat(statistic1.getKey().getValue()).isEqualTo("def3a909-f108-4008-bb55-c556b3d2ea89");
        Assertions.assertThat(statistic1.getKey().getStatisticName()).isEqualTo("ExampleCountStat");
        Assertions.assertThat(statistic1.getCount()).isEqualTo(2);
        Assertions.assertThat(statistic1.getTime().toString()).isEqualTo("2016-12-20T10:11:12.123Z");
        Assertions.assertThat(statistic1.getTags().getTag()).hasSize(2);
        Assertions.assertThat(statistic1.getTags().getTag().get(0).getName()).isEqualTo("department");
        Assertions.assertThat(statistic1.getTags().getTag().get(0).getValue()).isEqualTo("HR");
        Assertions.assertThat(statistic1.getTags().getTag().get(1).getName()).isEqualTo("user");
        Assertions.assertThat(statistic1.getTags().getTag().get(1).getValue()).isEqualTo("jbloggs");
    }

    @Test
    public void testMarshllUnmarshallToXml() throws Exception {

        Statistics.Statistic statistic = StatisticsHelper.buildCountStatistic(
                statUuid,
                statName,
                time,
                10L,
                StatisticsHelper.buildTagType(tag1, tag1val1),
                StatisticsHelper.buildTagType(tag2, tag2val1)
        );
        Statistics statistics1 = StatisticsHelper.buildStatistics(statistic);

        String xml = statisticsMarshaller.marshallToXml(statistics1);

        Statistics statistics2 = statisticsMarshaller.unMarshallFromXml(xml);

        Assertions.assertThat(statistics1.getStatistic())
                .hasSameSizeAs(statistics2.getStatistic());

        for (int i = 0; i < statistics1.getStatistic().size(); i++) {
            Statistics.Statistic statistic1 = statistics1.getStatistic().get(i);
            Statistics.Statistic statistic2 = statistics2.getStatistic().get(i);

            assertValue(statistic1, statistic2, stat -> stat.getKey().getValue());
            assertValue(statistic1, statistic2, stat -> stat.getKey().getStatisticName());
            assertValue(statistic1, statistic2, stat -> Long.toString(stat.getCount()));
            if (statistic1.getValue() != null) {
                assertValue(statistic1, statistic2, stat -> Double.toString(stat.getValue()));
            }
            assertValue(statistic1, statistic2, stat -> stat.getTime().toString());

            for (int j = 0; j < statistics1.getStatistic().size(); j++) {
                final int k = j;
                assertValue(statistic1, statistic2, stat -> stat.getTags().getTag().get(k).getName());
                assertValue(statistic1, statistic2, stat -> stat.getTags().getTag().get(k).getValue());
            }
        }
    }

    private void assertValue(final Statistics.Statistic statistic1,
                             final Statistics.Statistic statistic2,
                             Function<Statistics.Statistic, String> valueExtractionFunc) {
        String value1 = valueExtractionFunc.apply(statistic1);
        String value2 = valueExtractionFunc.apply(statistic2);
        Assertions.assertThat(value1).isEqualTo(value2);
    }


}