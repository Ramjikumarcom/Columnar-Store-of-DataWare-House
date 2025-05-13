import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;

/**
 * Represents a data cube for OLAP operations
 */
public class Cuboid {
    List<JoinedRecord> joinedRecords;
    private Map<String, DimensionTable> dimensionTables;
    private FactTable factTable;
    
    public Cuboid(FactTable factTable, Map<String, DimensionTable> dimensions) {
        this.joinedRecords = new ArrayList<>();
        this.dimensionTables = dimensions;
        this.factTable = factTable;
        for (int i = 0; i < factTable.size(); i++) {
            joinedRecords.add(new JoinedRecord(
                factTable.getOrdId(i),
                factTable.getProdId(i),
                factTable.getShipId(i),
                factTable.getCustId(i),
                i,
                factTable,
                dimensions
            ));
        }
    }
    
    public FactTable getFactTable() {
        return factTable;
    }
    
    public Map<String, DimensionTable> getDimensionTables() {
        return dimensionTables;
    }
    
    public List<JoinedRecord> filter(Predicate<JoinedRecord> predicate) {
        return joinedRecords.stream()
            .filter(predicate)
            .collect(Collectors.toList());
    }
    
    public <K, V> Map<K, V> groupByAndAggregate(
            Function<JoinedRecord, K> keyMapper,
            Function<List<JoinedRecord>, V> aggregator) {
        
        Map<K, List<JoinedRecord>> groupedRecords = joinedRecords.stream()
            .collect(Collectors.groupingBy(keyMapper));
        
        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, List<JoinedRecord>> entry : groupedRecords.entrySet()) {
            result.put(entry.getKey(), aggregator.apply(entry.getValue()));
        }
        
        return result;
    }
    
    public <K1, K2, V> Map<Map.Entry<K1, K2>, V> groupByTwoDimensionsAndAggregate(
            Function<JoinedRecord, K1> keyMapper1,
            Function<JoinedRecord, K2> keyMapper2,
            Function<List<JoinedRecord>, V> aggregator) {
        
        Map<Map.Entry<K1, K2>, List<JoinedRecord>> groupedRecords = new LinkedHashMap<>();
        
        for (JoinedRecord record : joinedRecords) {
            K1 key1 = keyMapper1.apply(record);
            K2 key2 = keyMapper2.apply(record);
            Map.Entry<K1, K2> compositeKey = new AbstractMap.SimpleEntry<>(key1, key2);
            
            groupedRecords.computeIfAbsent(compositeKey, _ -> new ArrayList<>()).add(record);
        }
        
        Map<Map.Entry<K1, K2>, V> result = new LinkedHashMap<>();
        for (Map.Entry<Map.Entry<K1, K2>, List<JoinedRecord>> entry : groupedRecords.entrySet()) {
            result.put(entry.getKey(), aggregator.apply(entry.getValue()));
        }
        
        return result;
    }

    public <K, V> Map<K, V> groupByMultipleDimensionsAndAggregate(
            Function<JoinedRecord, K> keyMapper,
            Function<List<JoinedRecord>, V> aggregator) {
        
        Map<K, List<JoinedRecord>> groupedRecords = new LinkedHashMap<>();
        
        for (JoinedRecord record : joinedRecords) {
            K key = keyMapper.apply(record);
            groupedRecords.computeIfAbsent(key, _ -> new ArrayList<>()).add(record);
        }
        
        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, List<JoinedRecord>> entry : groupedRecords.entrySet()) {
            result.put(entry.getKey(), aggregator.apply(entry.getValue()));
        }
        
        return result;
    }
}
