package stroom.stats.correlation;

import com.google.common.collect.Sets;
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

    private static final Set<Row> A = Sets.newHashSet(
        new Row("groupKey", Arrays.asList("user1", "door1"), 1),
        new Row("groupKey", Arrays.asList("user2", "door1"), 1),
        new Row("groupKey", Arrays.asList("user3", "door1"), 1));

    private static final Set<Row> B = Sets.newHashSet(
        new Row("groupKey", Arrays.asList("user1", "door1"), 1),
        new Row("groupKey", Arrays.asList("user2", "door1"), 1));

    private static final Set<Row> C = Sets.newHashSet(
        new Row("groupKey", Arrays.asList("user1", "door1"), 1),
        new Row("groupKey", Arrays.asList("user2", "door1"), 1),
        new Row("groupKey", Arrays.asList("user3", "door1"), 1),
        new Row("groupKey", Arrays.asList("user4", "door1"), 1),
        new Row("groupKey", Arrays.asList("user5", "door1"), 1),
        new Row("groupKey", Arrays.asList("user6", "door1"), 1),
        new Row("groupKey", Arrays.asList("user7", "door1"), 1));

    private static final Set<Row> D = Sets.newHashSet(
        new Row("groupKey", Arrays.asList("user1", "door1"), 1),
        new Row("groupKey", Arrays.asList("user2", "door1"), 1),
        new Row("groupKey", Arrays.asList("user3", "door1"), 1));

    private static final Set<Row> E = Sets.newHashSet(
        new Row("groupKey", Arrays.asList("user2", "door1"), 1));

    @Test
    public void testComplement(){
        List<Row> complementOfB = new Correlator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .complement(SET_B);

        assertThat(complementOfB.size()).isEqualTo(1);
        assertThat(complementOfB.get(0).getValues().get(0)).isEqualTo("user3");
        assertThat(complementOfB.get(0).getValues().get(1)).isEqualTo("door1");
    }

    @Test
    public void test_intersection_with_2_sets(){
        Set<Row> intersectionOfAandB = new Correlator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .intersection(SET_A, SET_B);
        List<Row> intersection = new ArrayList<>(intersectionOfAandB);

        assertThat(intersection.size()).isEqualTo(2);
        assertThat(intersection.get(0).getValues().get(0)).isEqualTo("user1");
        assertThat(intersection.get(1).getValues().get(0)).isEqualTo("user2");
    }

    @Test
    public void test_intersection_with_4_sets(){
        Set<Row> intersectionOfAandB = new Correlator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .addSet(SET_C, C)
                .addSet(SET_D, D)
                .intersection(SET_A, SET_B, SET_C, SET_D);
        List<Row> intersection = new ArrayList<>(intersectionOfAandB);

        assertThat(intersection.size()).isEqualTo(2);
        assertThat(intersection.get(0).getValues().get(0)).isEqualTo("user1");
        assertThat(intersection.get(1).getValues().get(0)).isEqualTo("user2");
    }


    @Test
    public void test_intersection_with_5_sets_small_intersection(){
        Set<Row> intersectionOfAandB = new Correlator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .addSet(SET_C, C)
                .addSet(SET_D, D)
                .addSet(SET_E, E)
                .intersection(SET_A, SET_B, SET_C, SET_D, SET_E);
        List<Row> intersection = new ArrayList<>(intersectionOfAandB);

        assertThat(intersection.size()).isEqualTo(1);
        assertThat(intersection.get(0).getValues().get(0)).isEqualTo("user2");
    }


    @Test
    public void test_intersection_with_5_sets_intersect_fewer(){
        Set<Row> intersectionOfAandB = new Correlator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .addSet(SET_C, C)
                .addSet(SET_D, D)
                .addSet(SET_E, E)
                .intersection(SET_C, SET_D);
        List<Row> intersection = new ArrayList<>(intersectionOfAandB);

        assertThat(intersection.size()).isEqualTo(3);
        assertThat(intersection.get(0).getValues().get(0)).isEqualTo("user1");
        assertThat(intersection.get(1).getValues().get(0)).isEqualTo("user2");
        assertThat(intersection.get(2).getValues().get(0)).isEqualTo("user3");
    }
}
