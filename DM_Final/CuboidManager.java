import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Main class for managing cuboid operations
 */
public class CuboidManager {
    private static final String BASE_PATH = "/home/ramji/Desktop/DM_Final/marketdb/";
    
    public static void main(String[] args) {
        try {
            Map<String, DimensionTable> dimensions = new HashMap<>();
            dimensions.put("customer", loadDimensionTableSchema("cust_dim", "Cust_id"));
            dimensions.put("product", loadDimensionTableSchema("prod_dim", "Prod_id"));
            dimensions.put("order", loadDimensionTableSchema("order_dim", "Ord_id"));
            dimensions.put("shipping", loadDimensionTableSchema("shipping_dim", "Ship_id"));
            
            FactTable factTable = loadFactTableSchema();
            factTable.setDimensionTables(dimensions); 
            
            for (DimensionTable dim : dimensions.values()) {
                dim.buildHashIndex();
            }
            
            List<String> latticeDimensions = XmlUtils.parseLatticeDimensions("newmarket.xml");
            System.out.println("Lattice dimensions found: " + latticeDimensions);
            
            Map<String, String> latticeDimensionToTableMap = mapLatticeDimensionsToTables(latticeDimensions, dimensions);
            
            denormalizeLatticeDimensions(factTable, latticeDimensions, latticeDimensionToTableMap);
            
            Cuboid baseCuboid = new Cuboid(factTable, dimensions);
            
            List<AggregationSpec> aggregationSpecs = XmlUtils.parseAggregationSpecs("newmarket.xml");
            System.out.println("Found " + aggregationSpecs.size() + " aggregation specifications");
            
            List<List<String>> allCombinations = generateAllCuboidCombinations(latticeDimensions);
            System.out.println("Generated " + allCombinations.size() + " cuboid combinations");
            
            Map<String, String> dimensionToTableMap = mapDimensionsToTables(dimensions);
            
            String cuboidDir = "cuboids/";
            
            System.out.println("\n===== Creating Cuboids Using Denormalized Columns =====");
            int skippedCount = 0;
            int createdCount = 0;
            for (List<String> combination : allCombinations) {
                if (FileUtils.cuboidFileExists(combination, cuboidDir)) {
                    System.out.println("Skipping existing cuboid: " + 
                                      (combination.isEmpty() ? "base_cuboid" : String.join("_", combination)));
                    skippedCount++;
                } else {
                    createAndSaveCuboid(baseCuboid, combination, latticeDimensionToTableMap, aggregationSpecs, cuboidDir);
                    createdCount++;
                }
            }
            System.out.println("Cuboid creation summary: " + createdCount + " created, " + 
                               skippedCount + " skipped (already existed)");
            
            boolean exit = false;
            Scanner scanner = new Scanner(System.in);
            
            while (!exit) {
                System.out.println("\n===== OLAP Operations Menu =====");
                System.out.println("1. Work with pre-computed cuboids");
                System.out.println("2. Recompute all cuboids (fresh computation)");
                System.out.println("3. Exit");
                System.out.print("Enter your choice (1-3): ");
                
                int choice = scanner.nextInt();
                scanner.nextLine();
                
                switch (choice) {
                    case 1:
                        performCuboidOperations(allCombinations, cuboidDir);
                        break;
                    case 2:
                        recomputeAllCuboids(latticeDimensions, cuboidDir);
                        break;
                    case 3:
                        exit = true;
                        break;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            }
            
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void recomputeAllCuboids(List<String> latticeDimensions, String cuboidDir) {
        try {
            System.out.println("\n===== Recomputing All Cuboids (Fresh Computation) =====");
            
            // Reload all dimension tables
            Map<String, DimensionTable> dimensions = new HashMap<>();
            dimensions.put("customer", loadDimensionTableSchema("cust_dim", "Cust_id"));
            dimensions.put("product", loadDimensionTableSchema("prod_dim", "Prod_id"));
            dimensions.put("order", loadDimensionTableSchema("order_dim", "Ord_id"));
            dimensions.put("shipping", loadDimensionTableSchema("shipping_dim", "Ship_id"));
            
            // Reload fact table
            FactTable factTable = loadFactTableSchema();
            factTable.setDimensionTables(dimensions);
            
            // Rebuild hash indexes
            for (DimensionTable dim : dimensions.values()) {
                dim.buildHashIndex();
            }
            
            // Re-map dimensions to tables
            Map<String, String> latticeDimensionToTableMap = mapLatticeDimensionsToTables(latticeDimensions, dimensions);
            
            // Use fresh denormalization instead of regular denormalization
            System.out.println("Performing fresh denormalization of dimension columns...");
            denormalizeFreshComputation(factTable, latticeDimensions, latticeDimensionToTableMap);
            
            // Create base cuboid
            Cuboid baseCuboid = new Cuboid(factTable, dimensions);
            
            // Load aggregation specifications
            List<AggregationSpec> aggregationSpecs = XmlUtils.parseAggregationSpecs("newmarket.xml");
            
            // Generate all possible cuboid combinations
            List<List<String>> allCombinations = generateAllCuboidCombinations(latticeDimensions);
            System.out.println("Will recompute all " + allCombinations.size() + " cuboids");
            
            // Create directory if it doesn't exist
            new File(cuboidDir).mkdirs();
            
            // Create all cuboids, overwriting existing ones
            int createdCount = 0;
            for (List<String> combination : allCombinations) {
                String cuboidName = combination.isEmpty() ? "base_cuboid" : String.join("_", combination);
                System.out.println("Recomputing cuboid: " + cuboidName);
                
                createAndSaveCuboid(baseCuboid, combination, latticeDimensionToTableMap, aggregationSpecs, cuboidDir);
                createdCount++;
            }
            
            System.out.println("Cuboid recomputation complete: " + createdCount + " cuboids freshly created");
            
        } catch (Exception e) {
            System.out.println("Error recomputing cuboids: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    // Add this new method to force fresh denormalization computation
    private static void denormalizeFreshComputation(FactTable factTable, List<String> latticeDimensions, 
                                                   Map<String, String> latticeDimensionToTableMap) {
        System.out.println("Forcing fresh computation of all denormalized columns...");
        
        for (String dimension : latticeDimensions) {
            String tableName = latticeDimensionToTableMap.get(dimension);
            if (tableName == null) {
                System.out.println("Warning: No table mapping for dimension: " + dimension);
                continue;
            }
            
            String columnKey = tableName + "." + dimension;
            System.out.println("Computing fresh denormalized column: " + columnKey);
            
            DimensionTable dimTable = factTable.getDimensionTables().get(tableName);
            if (dimTable == null) {
                System.out.println("Warning: Dimension table not found: " + tableName);
                continue;
            }
            
            // Fresh computation of denormalized column
            Object[] columnValues = new Object[factTable.size()];
            
            for (int i = 0; i < factTable.size(); i++) {
                String foreignKey = null;
                switch (tableName) {
                    case "customer": foreignKey = factTable.getCustId(i); break;
                    case "product": foreignKey = factTable.getProdId(i); break;
                    case "order": foreignKey = factTable.getOrdId(i); break;
                    case "shipping": foreignKey = factTable.getShipId(i); break;
                }
                
                columnValues[i] = dimTable.lookupValue(foreignKey, dimension);
            }
            
            // Determine if all values are numeric
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
            
            // Store in fact table
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
                factTable.denormalizedColumns.put(columnKey, numericValues);
            } else {
                factTable.denormalizedColumns.put(columnKey, columnValues);
            }
            
            // Still persist for future use
            factTable.persistDenormalizedColumn(columnKey, columnValues);
        }
        
        System.out.println("Fresh denormalization complete for all dimensions.");
    }
    
    private static DimensionTable loadDimensionTableSchema(String tableName, String keyColumn) throws IOException {
        String columnDir = BASE_PATH + tableName + "/";
        File dir = new File(columnDir);
        
        if (!dir.exists()) {
            throw new IOException("Directory not found: " + columnDir);
        }
        
        List<String> columnNames = Arrays.stream(dir.listFiles())
            .filter(f -> f.isFile() && f.getName().endsWith(".col"))
            .map(f -> f.getName().replace(".col", ""))
            .collect(Collectors.toList());
        
        Map<String, List<String>> keyColumnData = new HashMap<>();
        List<String> keyColumnValues = FileUtils.readBinaryColFile(columnDir + keyColumn + ".col");
        keyColumnData.put(keyColumn, keyColumnValues);
        
        return new DimensionTable(tableName, keyColumn, keyColumnData, columnNames);
    }
    
    private static FactTable loadFactTableSchema() throws IOException {
        String columnDir = BASE_PATH + "fact_table/";
        File dir = new File(columnDir);
        
        if (!dir.exists()) {
            throw new IOException("Directory not found: " + columnDir);
        }
        
        List<String> columnNames = Arrays.stream(dir.listFiles())
            .filter(f -> f.isFile() && f.getName().endsWith(".col"))
            .map(f -> f.getName().replace(".col", ""))
            .collect(Collectors.toList());
        
        List<String> ordIds = FileUtils.readBinaryColFile(columnDir + "Ord_id.col");
        List<String> prodIds = FileUtils.readBinaryColFile(columnDir + "Prod_id.col");
        List<String> shipIds = FileUtils.readBinaryColFile(columnDir + "Ship_id.col");
        List<String> custIds = FileUtils.readBinaryColFile(columnDir + "Cust_id.col");
        
        return new FactTable(ordIds, prodIds, shipIds, custIds, columnNames);
    }
    
    
    private static List<List<String>> generateAllCuboidCombinations(List<String> dimensions) {
        List<List<String>> result = new ArrayList<>();
        int n = dimensions.size();
        
        for (int i = 0; i < (1 << n); i++) {
            List<String> combination = new ArrayList<>();
            for (int j = 0; j < n; j++) {
                if ((i & (1 << j)) > 0) {
                    combination.add(dimensions.get(j));
                }
            }
            result.add(combination);
        }
        
        return result;
    }
    
    private static Map<String, String> mapDimensionsToTables(Map<String, DimensionTable> allDimensions) {
        Map<String, String> dimensionToTableMap = new HashMap<>();
        
        for (Map.Entry<String, DimensionTable> entry : allDimensions.entrySet()) {
            String tableName = entry.getKey();
            DimensionTable table = entry.getValue();
            
            for (String column : table.getColumnNames()) {
                dimensionToTableMap.put(column, tableName);
            }
        }
        
        return dimensionToTableMap;
    }
    
    private static void createAndSaveCuboid(Cuboid cuboid, List<String> dimensions, 
                                     Map<String, String> dimensionToTableMap,
                                     List<AggregationSpec> aggregationSpecs,
                                     String outputDir) throws IOException {
        String fileName = dimensions.isEmpty() ? "base_cuboid.csv" : 
                         String.join("_", dimensions).replaceAll("\\s+", "_") + ".csv";
        
        new File(outputDir).mkdirs();
        
        System.out.println("Creating cuboid: " + fileName);
        
        Map<String, List<JoinedRecord>> groupedRecords = new HashMap<>();
        
        for (JoinedRecord record : cuboid.joinedRecords) {
            StringBuilder key = new StringBuilder();
            for (String dimension : dimensions) {
                String tableName = dimensionToTableMap.get(dimension);
                if (tableName != null) {
                    String value = record.getDimensionValue(tableName, dimension);
                    key.append(value != null ? value : "null").append("|");
                }
            }
            
            groupedRecords.computeIfAbsent(key.toString(), _ -> new ArrayList<>())
                          .add(record);
        }
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputDir + fileName))) {
            List<String> header = new ArrayList<>(dimensions);
            
            for (AggregationSpec spec : aggregationSpecs) {
                for (String operation : spec.getOperations()) {
                    header.add(operation + "_" + spec.getFactColumn());
                }
            }
            
            writer.println(String.join(",", header));
            
            for (Map.Entry<String, List<JoinedRecord>> entry : groupedRecords.entrySet()) {
                List<String> rowValues = new ArrayList<>();
                
                String[] keyParts = entry.getKey().split("\\|");
                for (int i = 0; i < dimensions.size() && i < keyParts.length; i++) {
                    rowValues.add(keyParts[i]);
                }
                
                List<JoinedRecord> records = entry.getValue();
                
                for (AggregationSpec spec : aggregationSpecs) {
                    String factColumn = spec.getFactColumn();
                    
                    double sum = 0;
                    double max = Double.MIN_VALUE;
                    double min = Double.MAX_VALUE;
                    int count = 0;
                    
                    for (JoinedRecord record : records) {
                        Object value = record.getFactValue(factColumn);
                        if (value instanceof Number) {
                            double numValue = ((Number) value).doubleValue();
                            sum += numValue;
                            count++;
                            max = Math.max(max, numValue);
                            min = Math.min(min, numValue);
                        }
                    }
                    
                    for (String operation : spec.getOperations()) {
                        switch (operation) {
                            case "SUM":
                                rowValues.add(count > 0 ? String.format("%.2f", sum) : "0.00");
                                break;
                            case "AVG":
                                rowValues.add(count > 0 ? String.format("%.2f", sum/count) : "0.00");
                                break;
                            case "COUNT":
                                rowValues.add(String.valueOf(count));
                                break;
                            case "MAX":
                                rowValues.add(count > 0 ? String.format("%.2f", max != Double.MIN_VALUE ? max : 0) : "0.00");
                                break;
                            case "MIN":
                                rowValues.add(count > 0 ? String.format("%.2f", min != Double.MAX_VALUE ? min : 0) : "0.00");
                                break;
                        }
                    }
                }
                
                writer.println(String.join(",", rowValues));
            }
        }
        
        System.out.println("Saved cuboid to: " + outputDir + fileName);
    }
    
    private static void displayAvailableCuboids(List<List<String>> cuboids) {
        System.out.println("\n===== Available Cuboids =====");
        for (int i = 0; i < cuboids.size(); i++) {
            List<String> dimensions = cuboids.get(i);
            String description = dimensions.isEmpty() ? "Base cuboid (no dimensions)" : 
                               String.join(", ", dimensions);
            System.out.println((i+1) + ": " + description);
        }
    }
    
    /**
     * Loads and displays the contents of a cuboid file
     * Shows the dimensions, column headers, and the first 5 rows of data
     * 
     * @param dimensions List of dimension names for the cuboid
     * @param cuboidDir Directory containing cuboid files
     * @throws IOException If the cuboid file cannot be read
     */
    private static void loadAndDisplayCuboid(List<String> dimensions, String cuboidDir) throws IOException {
        // Generate filename based on dimensions (base_cuboid.csv for empty dimensions list)
        String fileName = dimensions.isEmpty() ? "base_cuboid.csv" : 
                         String.join("_", dimensions).replaceAll("\\s+", "_") + ".csv";
        
        File file = new File(cuboidDir + fileName);
        if (!file.exists()) {
            throw new IOException("Cuboid file not found: " + fileName);
        }
        
        System.out.println("\n===== Cuboid Data =====");
        System.out.println("Dimensions: " + String.join(", ", dimensions));
        
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String header = reader.readLine();
            System.out.println("\nColumns: " + header);
            
            System.out.println("\nSample Data (first 5 rows):");
            for (int i = 0; i < 5; i++) {
                String line = reader.readLine();
                if (line == null) break;
                System.out.println(line);
            }
        }
    }
    
    private static void performCuboidOperations(List<List<String>> cuboids, String cuboidDir) {
        Scanner scanner = new Scanner(System.in);
        boolean exit = false;
        
        while (!exit) {
            System.out.println("\n===== Cuboid Operations Menu =====");
            System.out.println("1. List available cuboids");
            System.out.println("2. Select a cuboid for operations");
            System.out.println("3. Return to main menu");
            
            int choice = scanner.nextInt();
            scanner.nextLine();
            
            switch (choice) {
                case 1:
                    displayAvailableCuboids(cuboids);
                    break;
                    
                case 2:
                    displayAvailableCuboids(cuboids);
                    System.out.print("Enter the number of the cuboid: ");
                    int cuboidChoice = scanner.nextInt();
                    scanner.nextLine();
                    
                    if (cuboidChoice >= 1 && cuboidChoice <= cuboids.size()) {
                        List<String> selectedDimensions = cuboids.get(cuboidChoice - 1);
                        try {
                            loadAndDisplayCuboid(selectedDimensions, cuboidDir);
                            performOperationsOnLoadedCuboid(selectedDimensions, cuboidDir);
                        } catch (IOException e) {
                            System.out.println("Error loading cuboid: " + e.getMessage());
                        }
                    } else {
                        System.out.println("Invalid choice");
                    }
                    break;
                    
                case 3:
                    exit = true;
                    break;
                    
                default:
                    System.out.println("Invalid choice");
            }
        }
    }
    
    private static void performOperationsOnLoadedCuboid(List<String> dimensions, String cuboidDir) {
        Scanner scanner = new Scanner(System.in);
        boolean exit = false;
        
        while (!exit) {
            System.out.println("\n===== Operations on Selected Cuboid =====");
            System.out.println("1. Filter data");
            System.out.println("2. Return to cuboid selection");
            
            int choice = scanner.nextInt();
            scanner.nextLine();
            
            switch (choice) {
                case 1:
                    filterCuboidData(dimensions, cuboidDir);
                    break;
                    
                case 2:
                    exit = true;
                    break;
                    
                default:
                    System.out.println("Invalid choice");
            }
        }
    }
    
    /**
     * Interactive method that allows users to filter cuboid data with multiple conditions
     * Supports equality, inequality, greater than, and less than operators
     * 
     * @param dimensions List of dimensions in the current cuboid
     * @param cuboidDir Directory containing the cuboid files
     */
    private static void filterCuboidData(List<String> dimensions, String cuboidDir) {
        Scanner scanner = new Scanner(System.in);
        
        try {
            String fileName = dimensions.isEmpty() ? "base_cuboid.csv" : 
                             String.join("_", dimensions).replaceAll("\\s+", "_") + ".csv";
            String filePath = cuboidDir + fileName;
            
            List<String> headers = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String headerLine = reader.readLine();
                if (headerLine != null) {
                    headers = Arrays.asList(headerLine.split(","));
                }
            }
            
            if (headers.isEmpty()) {
                System.out.println("Error: Could not read column headers from the cuboid file.");
                return;
            }
            
            System.out.print("\nDo you want to filter on multiple columns (dicing)? (y/n): ");
            boolean multipleFilters = scanner.nextLine().trim().toLowerCase().startsWith("y");
            
            List<FilterCondition> filterConditions = new ArrayList<>();
            
            do {
                System.out.println("\nAvailable columns for filtering:");
                for (int i = 0; i < headers.size(); i++) {
                    System.out.println((i+1) + ". " + headers.get(i));
                }
                
                System.out.print("\nSelect column to filter on (number): ");
                int columnIndex = scanner.nextInt() - 1;
                scanner.nextLine();
                
                if (columnIndex < 0 || columnIndex >= headers.size()) {
                    System.out.println("Invalid column selection.");
                    continue;
                }
                
                System.out.println("\nSelect operator:");
                System.out.println("1. Equal to (=)");
                System.out.println("2. Not equal to (!=)");
                System.out.println("3. Greater than (>)");
                System.out.println("4. Less than (<)");
                
                int operatorChoice = scanner.nextInt();
                scanner.nextLine();
                
                // Convert numeric choice to operator symbol
                String operator;
                switch(operatorChoice) {
                    case 1: operator = "="; break;
                    case 2: operator = "!="; break;
                    case 3: operator = ">"; break;
                    case 4: operator = "<"; break;
                    default: 
                        System.out.println("Invalid operator selection.");
                        continue;
                }
                
                System.out.print("Enter filter value: ");
                String filterValue = scanner.nextLine();
                
                // Create and add the filter condition
                filterConditions.add(new FilterCondition(columnIndex, operator, filterValue));
                
                // Ask for additional filters if multiple filters were requested
                if (multipleFilters) {
                    System.out.print("\nAdd another filter? (y/n): ");
                    // Continue or break based on user input
                    if (!scanner.nextLine().trim().toLowerCase().startsWith("y")) {
                        break;
                    }
                } else {
                    break;
                }
                
            } while (true);
            
            applyMultipleFiltersAndDisplay(filePath, filterConditions, headers, cuboidDir);
            
        } catch (IOException e) {
            System.out.println("Error filtering data: " + e.getMessage());
        }
    }
    
    /**
     * Applies multiple filter conditions to a cuboid file and displays the filtered results
     * 
     * @param filePath Path to the cuboid CSV file to filter
     * @param filterConditions List of filter conditions to apply (combined with logical AND)
     * @param headers List of column headers from the CSV file
     * @param cuboidDir Directory containing the cuboid files
     */
    private static void applyMultipleFiltersAndDisplay(String filePath, List<FilterCondition> filterConditions, 
                                                     List<String> headers, String cuboidDir) {
        try {
            List<String[]> filteredData = new ArrayList<>();
            
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                // Skip header row
                reader.readLine();
                
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] values = line.split(",");
                    
                    // Check if the row matches all filter conditions (logical AND)
                    boolean allMatch = true;
                    for (FilterCondition condition : filterConditions) {
                        if (!condition.matches(values)) {
                            allMatch = false;
                            break;
                        }
                    }
                    
                    if (allMatch) {
                        filteredData.add(values);
                    }
                }
            }
            
            System.out.println("\nFiltered Results: " + filteredData.size() + " rows found");
            
            if (filteredData.isEmpty()) {
                System.out.println("No matching records found.");
                return;
            }
            // Display logic continues...
            
            System.out.println(String.join(",", headers));
            
            int displayLimit = Math.min(filteredData.size(), 10);
            for (int i = 0; i < displayLimit; i++) {
                System.out.println(String.join(",", filteredData.get(i)));
            }
            
            if (filteredData.size() > 10) {
                System.out.println("... and " + (filteredData.size() - 10) + " more rows");
            }
            
            System.out.print("\nSave filtered results to CSV? (y/n): ");
            Scanner scanner = new Scanner(System.in);
            String choice = scanner.nextLine().trim().toLowerCase();
            
            if (choice.equals("y")) {
                System.out.print("Enter output filename (without .csv): ");
                String outputName = scanner.nextLine().trim();
                String outputPath = cuboidDir + outputName + ".csv";
                
                try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {
                    writer.println(String.join(",", headers));
                    
                    for (String[] row : filteredData) {
                        writer.println(String.join(",", row));
                    }
                    
                    System.out.println("Saved to " + outputPath);
                }
            }
            
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
    
    private static Map<String, String> mapLatticeDimensionsToTables(List<String> latticeDimensions, Map<String, DimensionTable> dimensions) {
        Map<String, String> dimensionToTableMap = new HashMap<>();
        
        for (String dimension : latticeDimensions) {
            boolean found = false;
            for (Map.Entry<String, DimensionTable> entry : dimensions.entrySet()) {
                String tableName = entry.getKey();
                DimensionTable table = entry.getValue();
                
                if (table.getColumnNames().contains(dimension)) {
                    dimensionToTableMap.put(dimension, tableName);
                    found = true;
                    System.out.println("Mapped dimension '" + dimension + "' to table '" + tableName + "'");
                    break;
                }
            }
            
            if (!found) {
                System.out.println("Warning: Could not find table for dimension '" + dimension + "'");
            }
        }
        
        return dimensionToTableMap;
    }
    
    private static void denormalizeLatticeDimensions(FactTable factTable, List<String> latticeDimensions, 
                                                   Map<String, String> latticeDimensionToTableMap) {
        Set<String> columnsToPreload = new HashSet<>();
        
        for (String dimension : latticeDimensions) {
            String tableName = latticeDimensionToTableMap.get(dimension);
            if (tableName != null) {
                columnsToPreload.add(tableName + "." + dimension);
            }
        }
        
        if (!columnsToPreload.isEmpty()) {
            System.out.println("\n===== Denormalizing Lattice Dimensions =====");
            System.out.println("Columns to denormalize: " + columnsToPreload);
            factTable.prepareForQuery(columnsToPreload);
            System.out.println("All lattice dimensions have been denormalized.");
        }
    }
}
