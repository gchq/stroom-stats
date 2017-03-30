package stroom.stats.correlation;

import org.junit.Test;
import stroom.query.api.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCorrelator {

    @Test
    public void test(){
        List<Row> a = new ArrayList<>();
        a.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        List<Row> b = new ArrayList<>();
        b.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));

        List<Row> complementOfB = new Correlator()
                .addSet(Correlator.SetName.A, a)
                .addSet(Correlator.SetName.B, b)
                .complement(Correlator.SetName.B);

        assertThat(complementOfB.size()).isEqualTo(1);
        assertThat(complementOfB.get(0).getValues().get(0)).isEqualTo("user3");
        assertThat(complementOfB.get(0).getValues().get(1)).isEqualTo("door1");
    }
}
