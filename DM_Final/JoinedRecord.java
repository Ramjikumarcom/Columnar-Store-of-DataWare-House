import java.util.*;

/**
 * Represents a joined record between fact and dimension tables
 */
public class JoinedRecord {
    private Map<String, DimensionTable> dimensionTables;
    private FactTable factTable;
    private int factRowIndex;
    private String ordId;
    private String prodId;
    private String shipId;
    private String custId;
    
    public JoinedRecord(String ordId, String prodId, String shipId, String custId, 
                       int factRowIndex, FactTable factTable,
                       Map<String, DimensionTable> dimensionTables) {
        this.dimensionTables = dimensionTables;
        this.factTable = factTable;
        this.factRowIndex = factRowIndex;
        this.ordId = ordId;
        this.prodId = prodId;
        this.shipId = shipId;
        this.custId = custId;
    }
    
    public String getDimensionValue(String dimName, String columnName) {
        String columnKey = dimName + "." + columnName;
        Object denormalizedColumn = factTable.getDenormalizedColumn(columnKey);
        
        if (denormalizedColumn != null) {
            if (denormalizedColumn instanceof Object[]) {
                Object[] columnValues = (Object[]) denormalizedColumn;
                Object value = factRowIndex < columnValues.length ? columnValues[factRowIndex] : null;
                return value != null ? value.toString() : null;
            } else if (denormalizedColumn instanceof double[]) {
                double[] numericValues = (double[]) denormalizedColumn;
                return factRowIndex < numericValues.length ? 
                       String.valueOf(numericValues[factRowIndex]) : null;
            }
        }
        
        DimensionTable dim = dimensionTables.get(dimName);
        if (dim == null) {
            return null;
        }
        
        String key = null;
        switch (dimName) {
            case "order": key = ordId; break;
            case "product": key = prodId; break;
            case "shipping": key = shipId; break;
            case "customer": key = custId; break;
            default: return null;
        }
        
        return dim.lookupValue(key, columnName);
    }
    
    public double getSales() {
        Object salesValue = getFactValue("Sales");
        return salesValue instanceof Number ? ((Number) salesValue).doubleValue() : 0.0;
    }
    
    public Object getFactValue(String columnName) {
        return factTable.getFactValue(columnName, factRowIndex);
    }
    
    public Number getNumericFactValue(String columnName) {
        Object value = getFactValue(columnName);
        return value instanceof Number ? (Number) value : 0.0;
    }
    
    public Set<String> getFactColumnNames() {
        return factTable.getFactColumnNames();
    }
}
