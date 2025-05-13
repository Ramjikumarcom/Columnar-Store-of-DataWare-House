import java.io.*;
import java.util.*;

/**
 * Represents a fact table in the data warehouse
 */
public class FactTable {
    private List<String> ordIds;
    private List<String> prodIds;
    private List<String> shipIds;
    private List<String> custIds;
    private Map<String, List<Object>> loadedFactColumns;
    private List<String> factColumnNames;
    private String columnDir;
    Map<String, Object> denormalizedColumns = new HashMap<>();
    private Map<String, DimensionTable> dimensionTables;
    private static final String BASE_PATH = "E:\\DM_Final\\marketdb\\";
    
    public FactTable(List<String> ordIds, List<String> prodIds, List<String> shipIds, 
                    List<String> custIds, List<String> columnNames) {
        this.ordIds = ordIds;
        this.prodIds = prodIds;
        this.shipIds = shipIds;
        this.custIds = custIds;
        this.factColumnNames = columnNames;
        this.loadedFactColumns = new HashMap<>();
        this.columnDir = BASE_PATH + "fact_table/";
    }
    
    public void setDimensionTables(Map<String, DimensionTable> dimensionTables) {
        this.dimensionTables = dimensionTables;
    }
    
    public void prepareForQuery(Set<String> requiredColumns) {
        System.out.println("Preparing columns for query: " + String.join(", ", requiredColumns));
        
        releaseUnusedColumns(requiredColumns);
        
        for (String column : requiredColumns) {
            if (!denormalizedColumns.containsKey(column)) {
                if (isDimensionColumn(column)) {
                    if (!loadDenormalizedColumnFromFile(column)) {
                        createAndPersistDenormalizedColumn(column);
                    }
                } else {
                    loadFactColumn(column);
                }
            }
        }
    }
    
    private void releaseUnusedColumns(Set<String> neededColumns) {
        Set<String> columnsToRelease = new HashSet<>(denormalizedColumns.keySet());
        columnsToRelease.removeAll(neededColumns);
        
        for (String column : columnsToRelease) {
            denormalizedColumns.remove(column);
            System.out.println("Released column: " + column);
        }
        
        Set<String> factColumnsToRelease = new HashSet<>(loadedFactColumns.keySet());
        factColumnsToRelease.removeAll(neededColumns);
        
        for (String column : factColumnsToRelease) {
            loadedFactColumns.remove(column);
            System.out.println("Released fact column: " + column);
        }
    }
    
    private boolean isDimensionColumn(String column) {
        return column.contains(".");
    }
    
    private boolean loadDenormalizedColumnFromFile(String column) {
        String columnName = column.substring(column.indexOf('.') + 1);
        String safeFileName = columnName + ".col";
        File file = new File(columnDir + safeFileName);
        
        if (!file.exists()) {
            System.out.println("Denormalized column file not found: " + columnName);
            return false;
        }
        
        try {
            System.out.println("Loading pre-existing denormalized column: " + column);
            List<String> lines = new ArrayList<>();
            
            try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
                String columnType = dis.readUTF();
                int rowCount = dis.readInt();
                
                for (int i = 0; i < rowCount; i++) {
                    switch (columnType) {
                        case "Integer":
                            lines.add(String.valueOf(dis.readInt()));
                            break;
                        case "Float":
                            lines.add(String.valueOf(dis.readFloat()));
                            break;
                        case "String":
                            int length = dis.readInt();
                            byte[] bytes = new byte[length];
                            dis.readFully(bytes);
                            lines.add(new String(bytes));
                            break;
                        default:
                            int len = dis.readInt();
                            byte[] b = new byte[len];
                            dis.readFully(b);
                            lines.add(new String(b));
                    }
                }
            }
            
            boolean allNumeric = true;
            for (String line : lines) {
                if (!line.isEmpty()) {
                    try {
                        Double.parseDouble(line);
                    } catch (NumberFormatException e) {
                        allNumeric = false;
                        break;
                    }
                }
            }
            
            if (allNumeric) {
                double[] values = new double[lines.size()];
                for (int i = 0; i < lines.size(); i++) {
                    values[i] = lines.get(i).isEmpty() ? 0.0 : Double.parseDouble(lines.get(i));
                }
                denormalizedColumns.put(column, values);
            } else {
                Object[] values = new Object[lines.size()];
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    if (line.isEmpty()) {
                        values[i] = null;
                    } else {
                        try {
                            values[i] = Double.parseDouble(line);
                        } catch (NumberFormatException e) {
                            values[i] = line;
                        }
                    }
                }
                denormalizedColumns.put(column, values);
            }
            
            return true;
        } catch (IOException e) {
            System.err.println("Error loading denormalized column from file: " + e.getMessage());
            return false;
        }
    }
    
    private void createAndPersistDenormalizedColumn(String column) {
        String[] parts = column.split("\\.");
        String dimensionName = parts[0];
        String dimensionColumn = parts[1];
        
        DimensionTable dimTable = dimensionTables.get(dimensionName);
        if (dimTable == null) return;
        
        System.out.println("Creating denormalized column: " + column);
        
        Object[] columnValues = new Object[size()];
        
        for (int i = 0; i < size(); i++) {
            String foreignKey = null;
            switch (dimensionName) {
                case "customer": foreignKey = custIds.get(i); break;
                case "product": foreignKey = prodIds.get(i); break;
                case "order": foreignKey = ordIds.get(i); break;
                case "shipping": foreignKey = shipIds.get(i); break;
            }
            
            columnValues[i] = dimTable.lookupValue(foreignKey, dimensionColumn);
        }
        
        boolean allNumeric = true;
        for (Object value : columnValues) {
            if (value != null && !(value instanceof Number)) {
                try {
                    Double.parseDouble(value.toString());
                } catch (NumberFormatException e) {
                    allNumeric = false;
                    break;
                }
            }
        }
        
        if (allNumeric) {
            double[] numericValues = new double[columnValues.length];
            for (int i = 0; i < columnValues.length; i++) {
                if (columnValues[i] == null) {
                    numericValues[i] = 0.0;
                } else if (columnValues[i] instanceof Number) {
                    numericValues[i] = ((Number)columnValues[i]).doubleValue();
                } else {
                    numericValues[i] = Double.parseDouble(columnValues[i].toString());
                }
            }
            denormalizedColumns.put(column, numericValues);
        } else {
            denormalizedColumns.put(column, columnValues);
        }
        
        persistDenormalizedColumn(column, columnValues);
    }
    
    public void persistDenormalizedColumn(String column, Object[] values) {
        String columnName = column.substring(column.indexOf('.') + 1);
        String safeFileName = columnName + ".col";
        File file = new File(columnDir + safeFileName);
        
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file))) {
            boolean allNumeric = true;
            for (Object value : values) {
                if (value != null && !(value instanceof Number)) {
                    try {
                        Double.parseDouble(value.toString());
                    } catch (NumberFormatException e) {
                        allNumeric = false;
                        break;
                    }
                }
            }
            
            if (allNumeric) {
                dos.writeUTF("Float");
                dos.writeInt(values.length);
                
                for (Object value : values) {
                    float val = 0.0f;
                    if (value != null) {
                        if (value instanceof Number) {
                            val = ((Number)value).floatValue();
                        } else {
                            try {
                                val = Float.parseFloat(value.toString());
                            } catch (NumberFormatException e) {
                                val = 0.0f;
                            }
                        }
                    }
                    dos.writeFloat(val);
                }
            } else {
                dos.writeUTF("String");
                dos.writeInt(values.length);
                
                for (Object value : values) {
                    String strVal = value != null ? value.toString() : "";
                    byte[] bytes = strVal.getBytes();
                    dos.writeInt(bytes.length);
                    dos.write(bytes);
                }
            }
            
            System.out.println("Persisted denormalized column to disk: " + columnName);
        } catch (IOException e) {
            System.err.println("Error persisting denormalized column: " + e.getMessage());
        }
    }
    
    public Object getDenormalizedColumn(String columnKey) {
        return denormalizedColumns.get(columnKey);
    }
    
    public int size() {
        return ordIds.size();
    }
    
    public String getOrdId(int index) { return ordIds.get(index); }
    public String getProdId(int index) { return prodIds.get(index); }
    public String getShipId(int index) { return shipIds.get(index); }
    public String getCustId(int index) { return custIds.get(index); }
    
    public Object getFactValue(String columnName, int index) {
        if (!loadedFactColumns.containsKey(columnName) && 
            !columnName.equals("Ord_id") && 
            !columnName.equals("Prod_id") && 
            !columnName.equals("Ship_id") && 
            !columnName.equals("Cust_id")) {
            
            loadFactColumn(columnName);
        }
        
        if (loadedFactColumns.containsKey(columnName)) {
            List<Object> column = loadedFactColumns.get(columnName);
            return index < column.size() ? column.get(index) : null;
        }
        return null;
    }
    
    private void loadFactColumn(String columnName) {
        try {
            if (!factColumnNames.contains(columnName)) {
                return;
            }
            
            List<Object> values = new ArrayList<>();
            
            try (DataInputStream dis = new DataInputStream(new FileInputStream(columnDir + columnName + ".col"))) {
                String columnType = dis.readUTF();
                int rowCount = dis.readInt();
                
                for (int i = 0; i < rowCount; i++) {
                    switch (columnType) {
                        case "Integer":
                            values.add(dis.readInt());
                            break;
                        case "Float":
                            values.add(dis.readFloat());
                            break;
                        case "String":
                            int length = dis.readInt();
                            byte[] bytes = new byte[length];
                            dis.readFully(bytes);
                            values.add(new String(bytes));
                            break;
                        default:
                            int len = dis.readInt();
                            byte[] b = new byte[len];
                            dis.readFully(b);
                            values.add(new String(b));
                    }
                }
            }
            
            loadedFactColumns.put(columnName, values);
        } catch (IOException e) {
            System.err.println("Error loading fact column " + columnName + ": " + e.getMessage());
        }
    }
    
    public Set<String> getFactColumnNames() {
        return new HashSet<>(factColumnNames);
    }
    
    public Set<String> getLoadedColumnNames() {
        return loadedFactColumns.keySet();
    }

    public Map<String, DimensionTable> getDimensionTables() {
        return dimensionTables;
    }
}
