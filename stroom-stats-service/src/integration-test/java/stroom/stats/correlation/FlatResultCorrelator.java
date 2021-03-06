package stroom.stats.correlation;

import stroom.query.api.v2.Field;
import stroom.query.api.v2.FlatResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class FlatResultCorrelator implements Correlator<FlatResult, List<Object>>{

    private final BasicCorrelator<List<Object>> basicCorrelator;
    private List<Field> structure = Collections.emptyList();
    private final Comparator<Field> fieldComparator = Comparator.comparing(Field::getName);

    public FlatResultCorrelator() {
        basicCorrelator = new BasicCorrelator<>();
    }

    private FlatResult wrapRows(Collection<List<Object>> rows) {
        List<List<Object>> rowList = new ArrayList<>(rows);
        return new FlatResult("Unknown", structure, rowList, (long) rows.size(), "");
    }

    @Override
    public Correlator<FlatResult, List<Object>> addSet(final String setName, final FlatResult flatResult) {

        if (!isEmpty() && !validateStructure(flatResult)) {
            throw new RuntimeException(String.format("Structures don't match %s, %s", structure, flatResult.getStructure()));
        }
        structure = new ArrayList<>(flatResult.getStructure());

        basicCorrelator.addSet(setName, flatResult.getValues());
        return this;
    }

    @Override
    public FlatResult complement(final String setName) {
        return wrapRows(basicCorrelator.complement(setName));
    }

    @Override
    public FlatResult intersection(final String... setNames) {
        return wrapRows(basicCorrelator.intersection(setNames));
    }

    @Override
    public boolean isEmpty() {
        return basicCorrelator.isEmpty();
    }

    boolean validateStructure(final FlatResult flatResult) {
        if (flatResult.getStructure().size() != structure.size()){
            return false;
        }
        return IntStream.range(0, structure.size())
                .allMatch(i -> fieldComparator.compare(structure.get(i), flatResult.getStructure().get(i)) == 0);
    }
}
