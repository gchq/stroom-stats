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

    private static final String SET_A = "A";
    private static final String SET_B = "B";
    private static final String SET_C = "C";
    private static final String SET_D = "D";
    private static final String SET_E = "E";

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
                .addSet(SET_A, a)
                .addSet(SET_B, b)
                .complement(SET_B);

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
                .addSet(SET_A, a)
                .addSet(SET_B, b)
                .intersection(SET_A, SET_B);
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
                .addSet(SET_A, a)
                .addSet(SET_B, b)
                .addSet(SET_C, c)
                .addSet(SET_D, d)
                .intersection(SET_A, SET_B, SET_C, SET_D);
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
                .addSet(SET_A, a)
                .addSet(SET_B, b)
                .addSet(SET_C, c)
                .addSet(SET_D, d)
                .addSet(SET_E, e)
                .intersection(SET_A, SET_B, SET_C, SET_D, SET_E);
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
                .addSet(SET_A, a)
                .addSet(SET_B, b)
                .addSet(SET_C, c)
                .addSet(SET_D, d)
                .addSet(SET_E, e)
                .intersection(SET_B, SET_C);
        List<Row> intersection = new ArrayList<>(intersectionOfAandB);

        assertThat(intersection.size()).isEqualTo(5);
        assertThat(intersection.get(0).getValues().get(0)).isEqualTo("user1");
        assertThat(intersection.get(1).getValues().get(0)).isEqualTo("user2");
        assertThat(intersection.get(2).getValues().get(0)).isEqualTo("user3");
        assertThat(intersection.get(3).getValues().get(0)).isEqualTo("user4");
        assertThat(intersection.get(4).getValues().get(0)).isEqualTo("user5");
    }
}
