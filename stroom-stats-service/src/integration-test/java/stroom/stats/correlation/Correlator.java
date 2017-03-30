package stroom.stats.correlation;

import com.google.common.collect.ImmutableList;
import stroom.query.api.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Correlator {

    private Map<SetName, ImmutableList<Row>> sets = new HashMap<>();

    public Correlator addSet(SetName setName, List<Row> set){
        sets.put(setName, ImmutableList.copyOf(set));
        return this;
    }

    public List<Row> complement(SetName setName){
        ImmutableList<Row> set = sets.get(setName);
        List<Row> complementOfSet = sets.entrySet().stream()
                .filter(entry -> entry.getKey() != setName) // Get rid of our complement set
                .flatMap(entry -> entry.getValue().stream()) // We don't care about sets now, just about rows
                .filter(row -> !set.contains(row)) // We only want stuff not in our main set
                .collect(Collectors.toList());

        return complementOfSet;
    }

    public enum SetName {
        A, B, C, D, E;
    }
}
