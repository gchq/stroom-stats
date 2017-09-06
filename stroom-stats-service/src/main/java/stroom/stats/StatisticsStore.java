package stroom.stats;

import stroom.mapreduce.v2.UnsafePairQueue;
import stroom.query.api.v2.TableSettings;
import stroom.query.common.v2.CompiledSorter;
import stroom.query.common.v2.Coprocessor;
import stroom.query.common.v2.CoprocessorSettingsMap;
import stroom.query.common.v2.Data;
import stroom.query.common.v2.Item;
import stroom.query.common.v2.Key;
import stroom.query.common.v2.Payload;
import stroom.query.common.v2.ResultStoreCreator;
import stroom.query.common.v2.Store;
import stroom.query.common.v2.StoreSize;
import stroom.query.common.v2.TableCoprocessorSettings;
import stroom.query.common.v2.TablePayload;

import java.util.List;
import java.util.Map;

public class StatisticsStore implements Store {

    private CoprocessorSettingsMap coprocessorSettingsMap;
    private Map<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> coprocessorMap;
    private Map<CoprocessorSettingsMap.CoprocessorKey, Payload> payloadMap;

    private final List<Integer> defaultMaxResultsSizes;
    private final StoreSize storeSize;

    public StatisticsStore(final List<Integer> defaultMaxResultsSizes, final StoreSize storeSize) {
        this.defaultMaxResultsSizes = defaultMaxResultsSizes;
        this.storeSize = storeSize;
    }

    @Override
    public void destroy() {

    }

    @Override
    public boolean isComplete() {
        return true;
    }

    @Override
    public Data getData(String componentId) {
        final CoprocessorSettingsMap.CoprocessorKey coprocessorKey = coprocessorSettingsMap.getCoprocessorKey(componentId);
        if (coprocessorKey == null) {
            return null;
        }

        TableCoprocessorSettings tableCoprocessorSettings = (TableCoprocessorSettings) coprocessorSettingsMap.getMap()
                .get(coprocessorKey);
        TableSettings tableSettings = tableCoprocessorSettings.getTableSettings();

        Payload payload = payloadMap.get(coprocessorKey);
        TablePayload tablePayload = (TablePayload) payload;
        UnsafePairQueue<Key, Item> queue = tablePayload.getQueue();

        CompiledSorter compiledSorter = new CompiledSorter(tableSettings.getFields());
        final ResultStoreCreator resultStoreCreator = new ResultStoreCreator(compiledSorter);
        resultStoreCreator.read(queue);

        // Trim the number of results in the store.
        resultStoreCreator.trim(storeSize);

        return resultStoreCreator.create(queue.size(), queue.size());
    }

    @Override
    public List<String> getErrors() {
        return null;
    }

    @Override
    public List<String> getHighlights() {
        return null;
    }

    @Override
    public List<Integer> getDefaultMaxResultsSizes() {
        return defaultMaxResultsSizes;
    }

    @Override
    public StoreSize getStoreSize() {
        return storeSize;
    }

    public void process(CoprocessorSettingsMap coprocessorSettingsMap) {
        this.coprocessorSettingsMap = coprocessorSettingsMap;
    }

    public void coprocessorMap(Map<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> coprocessorMap) {
        this.coprocessorMap = coprocessorMap;
    }

    public void payloadMap(Map<CoprocessorSettingsMap.CoprocessorKey, Payload> payloadMap) {
        this.payloadMap = payloadMap;
    }
}
