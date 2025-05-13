import java.util.*;

/**
 * Represents a dimension table in the data warehouse
 */
public class DimensionTable {
    private String name;
    private String keyColumn;
    private Map<String, List<String>> loadedColumns;
    private List<String> columnNames;
    private Map<String, Integer> keyToRowIndex;
    private String columnDir;
    private static final String BASE_PATH = "E:\\DM_Final\\marketdb\\";
    
    public DimensionTable(String name, String keyColumn, Map<String, List<String>> initialColumns, 
                          List<String> columnNames) {
        this.name = name;
        this.keyColumn = keyColumn;
        this.loadedColumns = initialColumns;
        this.columnNames = columnNames;
        this.keyToRowIndex = new HashMap<>();
        this.columnDir = BASE_PATH + name + "/";
    }
    
    public void buildHashIndex() {
        List<String> keys = loadedColumns.get(keyColumn);
        for (int i = 0; i < keys.size(); i++) {
            keyToRowIndex.put(keys.get(i), i);
        }
    }
    
    public String lookupValue(String key, String columnName) {
        if (!loadedColumns.containsKey(columnName)) {
            loadColumn(columnName);
        }
        
        Integer rowIndex = keyToRowIndex.get(key);
        if (rowIndex == null || !loadedColumns.containsKey(columnName)) {
            return null;
        }
        
        List<String> column = loadedColumns.get(columnName);
        if (rowIndex >= column.size()) {
            return null;
        }
        return column.get(rowIndex);
    }
    
    private void loadColumn(String columnName) {
        try {
            if (!columnNames.contains(columnName)) {
                return;
            }
            
            List<String> columnData = FileUtils.readBinaryColFile(columnDir + columnName + ".col");
            loadedColumns.put(columnName, columnData);
        } catch (Exception e) {
            System.err.println("Error loading column " + columnName + ": " + e.getMessage());
        }
    }
    
    public String getName() {
        return name;
    }
    
    public Set<String> getColumnNames() {
        return new HashSet<>(columnNames);
    }
    
    public Set<String> getLoadedColumnNames() {
        return loadedColumns.keySet();
    }
}
