package stroom.stats.correlation;

import org.junit.Test;
import stroom.query.api.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCorrelator {

    @Test
    public void testComplement(){
        Set<Row> a = new HashSet<>();
        a.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        Set<Row> b = new HashSet<>();
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

    @Test
    public void test_intersection_with_2_sets(){
        Set<Row> a = new HashSet<>();
        a.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        Set<Row> b = new HashSet<>();
        b.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));

        Set<Row> intersectionOfAandB = new Correlator()
                .addSet(Correlator.SetName.A, a)
                .addSet(Correlator.SetName.B, b)
                .intersection(Correlator.SetName.A, Correlator.SetName.B);
        List<Row> intersection = new ArrayList<>(intersectionOfAandB);

        assertThat(intersection.size()).isEqualTo(2);
        assertThat(intersection.get(0).getValues().get(0)).isEqualTo("user1");
        assertThat(intersection.get(1).getValues().get(0)).isEqualTo("user2");
    }

    @Test
    public void test_intersection_with_4_sets(){
        Set<Row> a = new HashSet<>();
        a.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user4", "door1"), 1));
        Set<Row> b = new HashSet<>();
        b.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user4", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user5", "door1"), 1));
        Set<Row> c = new HashSet<>();
        c.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user4", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user5", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user6", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user7", "door1"), 1));
        Set<Row> d = new HashSet<>();
        d.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        d.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        d.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        Set<Row> intersectionOfAandB = new Correlator()
                .addSet(Correlator.SetName.A, a)
                .addSet(Correlator.SetName.B, b)
                .addSet(Correlator.SetName.C, c)
                .addSet(Correlator.SetName.D, d)
                .intersection(Correlator.SetName.A, Correlator.SetName.B, Correlator.SetName.C, Correlator.SetName.D);
        List<Row> intersection = new ArrayList<>(intersectionOfAandB);

        assertThat(intersection.size()).isEqualTo(3);
        assertThat(intersection.get(0).getValues().get(0)).isEqualTo("user1");
        assertThat(intersection.get(1).getValues().get(0)).isEqualTo("user2");
    }


    @Test
    public void test_intersection_with_5_sets_small_intersection(){
        Set<Row> a = new HashSet<>();
        a.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user4", "door1"), 1));
        Set<Row> b = new HashSet<>();
        b.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user4", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user5", "door1"), 1));
        Set<Row> c = new HashSet<>();
        c.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user4", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user5", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user6", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user7", "door1"), 1));
        Set<Row> d = new HashSet<>();
        d.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        d.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        d.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        Set<Row> e = new HashSet<>();
        e.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        Set<Row> intersectionOfAandB = new Correlator()
                .addSet(Correlator.SetName.A, a)
                .addSet(Correlator.SetName.B, b)
                .addSet(Correlator.SetName.C, c)
                .addSet(Correlator.SetName.D, d)
                .addSet(Correlator.SetName.E, e)
                .intersection(Correlator.SetName.A, Correlator.SetName.B, Correlator.SetName.C, Correlator.SetName.D, Correlator.SetName.E);
        List<Row> intersection = new ArrayList<>(intersectionOfAandB);

        assertThat(intersection.size()).isEqualTo(1);
        assertThat(intersection.get(0).getValues().get(0)).isEqualTo("user3");
    }


    @Test
    public void test_intersection_with_5_sets_intersect_fewer(){
        Set<Row> a = new HashSet<>();
        a.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        a.add(new Row("groupKey", Arrays.asList("user4", "door1"), 1));
        Set<Row> b = new HashSet<>();
        b.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user4", "door1"), 1));
        b.add(new Row("groupKey", Arrays.asList("user5", "door1"), 1));
        Set<Row> c = new HashSet<>();
        c.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user4", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user5", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user6", "door1"), 1));
        c.add(new Row("groupKey", Arrays.asList("user7", "door1"), 1));
        Set<Row> d = new HashSet<>();
        d.add(new Row("groupKey", Arrays.asList("user1", "door1"), 1));
        d.add(new Row("groupKey", Arrays.asList("user2", "door1"), 1));
        d.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        Set<Row> e = new HashSet<>();
        e.add(new Row("groupKey", Arrays.asList("user3", "door1"), 1));
        Set<Row> intersectionOfAandB = new Correlator()
                .addSet(Correlator.SetName.A, a)
                .addSet(Correlator.SetName.B, b)
                .addSet(Correlator.SetName.C, c)
                .addSet(Correlator.SetName.D, d)
                .addSet(Correlator.SetName.E, e)
                .intersection(Correlator.SetName.B, Correlator.SetName.C);
        List<Row> intersection = new ArrayList<>(intersectionOfAandB);

        assertThat(intersection.size()).isEqualTo(5);
        assertThat(intersection.get(0).getValues().get(0)).isEqualTo("user1");
        assertThat(intersection.get(1).getValues().get(0)).isEqualTo("user2");
        assertThat(intersection.get(2).getValues().get(0)).isEqualTo("user3");
        assertThat(intersection.get(3).getValues().get(0)).isEqualTo("user4");
        assertThat(intersection.get(4).getValues().get(0)).isEqualTo("user5");
    }
}
