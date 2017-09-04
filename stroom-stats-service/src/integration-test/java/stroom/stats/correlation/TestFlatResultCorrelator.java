package stroom.stats.correlation;

import org.junit.Test;
import stroom.query.api.v2.Field;
import stroom.query.api.v2.FlatResult;
import stroom.query.api.v2.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFlatResultCorrelator {

    private static final String SET_A = "A";
    private static final String SET_B = "B";
    private static final String SET_C = "C";
    private static final String SET_D = "D";
    private static final String SET_E = "E";

    private static final List<Object> USER1_DOOR1 = Arrays.asList("user1", "door1");
    private static final List<Object> USER2_DOOR1 = Arrays.asList("user2", "door1");
    private static final List<Object> USER3_DOOR1 = Arrays.asList("user3", "door1");
    private static final List<Object> USER4_DOOR1 = Arrays.asList("user4", "door1");
    private static final List<Object> USER5_DOOR1 = Arrays.asList("user5", "door1");
    private static final List<Object> USER6_DOOR1 = Arrays.asList("user6", "door1");
    private static final List<Object> USER7_DOOR1 = Arrays.asList("user7", "door1");

    private static final List<List<Object>> LIST_A = Arrays.asList(
            new ArrayList<>(USER1_DOOR1),
            new ArrayList<>(USER2_DOOR1),
            new ArrayList<>(USER3_DOOR1));

    private static final List<List<Object>> LIST_B = Arrays.asList(
            new ArrayList<>(USER1_DOOR1),
            new ArrayList<>(USER2_DOOR1));

    private static final List<List<Object>> LIST_C = Arrays.asList(
            new ArrayList<>(USER1_DOOR1),
            new ArrayList<>(USER2_DOOR1),
            new ArrayList<>(USER3_DOOR1),
            new ArrayList<>(USER4_DOOR1),
            new ArrayList<>(USER5_DOOR1),
            new ArrayList<>(USER6_DOOR1),
            new ArrayList<>(USER7_DOOR1));

    private static final List<List<Object>> LIST_D = Arrays.asList(
            new ArrayList<>(USER1_DOOR1),
            new ArrayList<>(USER2_DOOR1),
            new ArrayList<>(USER3_DOOR1));

    private static final Set<List<Object>> LIST_E = Collections.singleton(
            new ArrayList<>(USER2_DOOR1));

    private static final FlatResult A = wrapRows(LIST_A);
    private static final FlatResult B = wrapRows(LIST_B);
    private static final FlatResult C = wrapRows(LIST_C);
    private static final FlatResult D = wrapRows(LIST_D);
    private static final FlatResult E = wrapRows(LIST_E);

    private static Set<Row> asRows(Set<List<Object>> rows) {
        return rows.stream()
                .map(values -> new Row("groupKey",
                        values.stream()
                                .map(Object::toString)
                                .collect(Collectors.toList()),
                        1))
                .collect(Collectors.toSet());
    }

    @Test
    public void testComplement() {
        FlatResult complementOfB = new FlatResultCorrelator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .complement(SET_B);

        assertThat(complementOfB.getValues()).hasSize(1);
        assertThat(complementOfB.getValues().get(0).get(0)).isEqualTo("user3");
        assertThat(complementOfB.getValues().get(0).get(1)).isEqualTo("door1");
    }

    @Test
    public void test_intersection_with_2_sets() {
        FlatResult intersectionOfAandB = new FlatResultCorrelator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .intersection(SET_A, SET_B);

        assertThat(intersectionOfAandB.getValues()).hasSize(2);
        assertThat(intersectionOfAandB.getValues()).containsExactlyInAnyOrder(
                USER1_DOOR1,
                USER2_DOOR1
        );
    }

    @Test
    public void test_intersection_with_4_sets() {
        FlatResult intersectionOfAandB = new FlatResultCorrelator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .addSet(SET_C, C)
                .addSet(SET_D, D)
                .intersection(SET_A, SET_B, SET_C, SET_D);

        assertThat(intersectionOfAandB.getValues()).hasSize(2);
        assertThat(intersectionOfAandB.getValues()).containsExactlyInAnyOrder(
                USER1_DOOR1,
                USER2_DOOR1
        );
    }

    @Test
    public void test_intersection_with_5_sets_small_intersection() {
        FlatResult intersectionOfAandB = new FlatResultCorrelator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .addSet(SET_C, C)
                .addSet(SET_D, D)
                .addSet(SET_E, E)
                .intersection(SET_A, SET_B, SET_C, SET_D, SET_E);

        assertThat(intersectionOfAandB.getValues()).hasSize(1);
        assertThat(intersectionOfAandB.getValues()).containsExactly(USER2_DOOR1);
    }

    @Test
    public void test_intersection_with_5_sets_intersect_fewer() {
        FlatResult intersectionOfAandB = new FlatResultCorrelator()
                .addSet(SET_A, A)
                .addSet(SET_B, B)
                .addSet(SET_C, C)
                .addSet(SET_D, D)
                .addSet(SET_E, E)
                .intersection(SET_C, SET_D);

        assertThat(intersectionOfAandB.getValues()).hasSize(3);
        assertThat(intersectionOfAandB.getValues()).containsExactlyInAnyOrder(
                USER1_DOOR1,
                USER2_DOOR1,
                USER3_DOOR1);

//        List<List<Object>> intersection = new ArrayList<>(intersectionOfAandB);
//
//        assertThat(intersection.size()).isEqualTo(3);
//        assertThat(intersection.get(0).get(0)).isEqualTo("user1");
//        assertThat(intersection.get(1).get(0)).isEqualTo("user2");
//        assertThat(intersection.get(2).get(0)).isEqualTo("user3");
    }

    private static FlatResult wrapRows(Collection<List<Object>> rows) {
        List<List<Object>> rowList = new ArrayList<>(rows);

        List<Field> structure = Arrays.asList(
                new Field("User", "${user}", null, null, null, null),
                new Field("Door", "${door}", null, null, null, null));

        return new FlatResult("Unknown", structure, rowList, (long) rows.size(), "");
    }
}
