import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvToColFiles {
    public static void main(String[] args) {
        boolean appendMode = false;
        String csvFilename = "Input_Data/shipping_dim.csv"; // Input CSV file
        String colFilesFolder = "marketdb/shipping_dim/"; // Folder with existing .col files

        // Check if append mode is requested
        if (args.length > 0 && args[0].equals("--append")) {
            appendMode = true;
            System.out.println("Running in APPEND mode");
        }

        boolean success;
        if (appendMode) {
            success = appendCsvDataToColFiles(csvFilename, colFilesFolder);
            if (success) {
                System.out.println("Successfully appended CSV data to existing .col files");
            } else {
                System.out.println("Failed to append CSV data to .col files");
            }
        } else {
            success = insertCsvDataIntoColFiles(csvFilename, colFilesFolder);
            if (success) {
                System.out.println("Successfully inserted CSV data into existing .col files");
            } else {
                System.out.println("Failed to insert CSV data into .col files");
            }
        }
    }

    public static boolean insertCsvDataIntoColFiles(String csvFilename, String colFilesFolder) {
        try {
            // Get list of existing .col files
            File folder = new File(colFilesFolder);
            File[] colFiles = folder.listFiles((_, name) -> name.endsWith(".col"));
            
            if (colFiles == null || colFiles.length == 0) {
                System.out.println("No .col files found in " + colFilesFolder);
                return false;
            }
            
            // Create a map of column names to file paths
            Map<String, String> colFileMap = new HashMap<>();
            for (File file : colFiles) {
                String columnName = file.getName().replace(".col", "");
                colFileMap.put(columnName, file.getAbsolutePath());
            }
            
            // Read CSV header and data
            BufferedReader br = new BufferedReader(new FileReader(csvFilename));
            String headerLine = br.readLine();
            
            if (headerLine == null) {
                System.out.println("Empty CSV file.");
                br.close();
                return false;
            }
            
            // Parse CSV headers
            String[] headers = headerLine.split(",");
            
            // Prepare data structures for each column
            Map<String, List<String>> columnData = new HashMap<>();
            for (String header : headers) {
                String trimmedHeader = header.trim();
                if (colFileMap.containsKey(trimmedHeader)) {
                    columnData.put(trimmedHeader, new ArrayList<>());
                } else {
                    System.out.println("Warning: No matching .col file found for column: " + trimmedHeader);
                }
            }
            
            // Read CSV data
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                for (int i = 0; i < headers.length && i < values.length; i++) {
                    String header = headers[i].trim();
                    if (columnData.containsKey(header)) {
                        columnData.get(header).add(values[i].trim());
                    }
                }
            }
            br.close();
            
            // Write data to existing .col files
            for (String columnName : columnData.keySet()) {
                String filePath = colFileMap.get(columnName);
                if (filePath != null) {
                    List<String> values = columnData.get(columnName);
                    String columnType = detectColumnType(values);
                    writeToColFile(filePath, values, columnType);
                }
            }
            
            return true;
        } catch (IOException e) {
            System.out.println("Error processing files: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public static boolean appendCsvDataToColFiles(String csvFilename, String colFilesFolder) {
        try {
            // Get list of existing .col files
            File folder = new File(colFilesFolder);
            File[] colFiles = folder.listFiles((_, name) -> name.endsWith(".col"));
            
            if (colFiles == null || colFiles.length == 0) {
                System.out.println("No .col files found in " + colFilesFolder);
                return false;
            }
            
            // Create a map of column names to file paths
            Map<String, String> colFileMap = new HashMap<>();
            // Also create a map to store existing data and types
            Map<String, List<String>> existingColumnData = new HashMap<>();
            Map<String, String> columnTypes = new HashMap<>();
            
            for (File file : colFiles) {
                String columnName = file.getName().replace(".col", "");
                String filePath = file.getAbsolutePath();
                colFileMap.put(columnName, filePath);
                
                // Read existing data from each column file
                ColumnFileData columnData = readFromColFile(filePath);
                existingColumnData.put(columnName, columnData.values);
                columnTypes.put(columnName, columnData.type);
            }
            
            // Read CSV header and data
            BufferedReader br = new BufferedReader(new FileReader(csvFilename));
            String headerLine = br.readLine();
            
            if (headerLine == null) {
                System.out.println("Empty CSV file.");
                br.close();
                return false;
            }
            
            // Parse CSV headers
            String[] headers = headerLine.split(",");
            
            // Prepare data structures for new column data
            Map<String, List<String>> newColumnData = new HashMap<>();
            for (String header : headers) {
                String trimmedHeader = header.trim();
                if (colFileMap.containsKey(trimmedHeader)) {
                    newColumnData.put(trimmedHeader, new ArrayList<>());
                } else {
                    System.out.println("Warning: No matching .col file found for column: " + trimmedHeader);
                }
            }
            
            // Read new CSV data
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                for (int i = 0; i < headers.length && i < values.length; i++) {
                    String header = headers[i].trim();
                    if (newColumnData.containsKey(header)) {
                        newColumnData.get(header).add(values[i].trim());
                    }
                }
            }
            br.close();
            
            // Append new data to existing data and write back to .col files
            for (String columnName : newColumnData.keySet()) {
                String filePath = colFileMap.get(columnName);
                if (filePath != null) {
                    List<String> existingValues = existingColumnData.get(columnName);
                    List<String> newValues = newColumnData.get(columnName);
                    String columnType = columnTypes.get(columnName);
                    
                    // Validate that new data is compatible with existing column type
                    String newDataType = detectColumnType(newValues);
                    if (!columnType.equals(newDataType) && !newValues.isEmpty()) {
                        System.out.println("Warning: Data type mismatch for column " + columnName + 
                                          ". Existing: " + columnType + ", New: " + newDataType);
                        
                        // Convert data if possible or use the more general type
                        if ((columnType.equals("Integer") && newDataType.equals("Float")) || 
                            (columnType.equals("Float") && newDataType.equals("Integer"))) {
                            columnType = "Float"; // Upgrade to Float
                        } else {
                            columnType = "String"; // Fallback to String
                        }
                    }
                    
                    // Combine existing and new values
                    List<String> combinedValues = new ArrayList<>(existingValues);
                    combinedValues.addAll(newValues);
                    
                    // Write combined data back to the .col file
                    writeToColFile(filePath, combinedValues, columnType);
                    
                    System.out.println("Appended " + newValues.size() + " new values to column " + 
                                      columnName + " (total: " + combinedValues.size() + ")");
                }
            }
            
            return true;
        } catch (IOException e) {
            System.out.println("Error processing files: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // Class to hold column file data
    private static class ColumnFileData {
        String type;
        List<String> values;
        
        public ColumnFileData(String type, List<String> values) {
            this.type = type;
            this.values = values;
        }
    }
    
    // Function to read data from existing .col file
    private static ColumnFileData readFromColFile(String filename) throws IOException {
        List<String> values = new ArrayList<>();
        String type;
        
        try (DataInputStream dis = new DataInputStream(new FileInputStream(filename))) {
            type = dis.readUTF(); // Read column type
            int rowCount = dis.readInt(); // Read row count
            
            for (int i = 0; i < rowCount; i++) {
                switch (type) {
                    case "Integer" -> values.add(String.valueOf(dis.readInt()));
                    case "Float" -> values.add(String.valueOf(dis.readFloat()));
                    case "String" -> {
                        int strLength = dis.readInt();
                        byte[] bytes = new byte[strLength];
                        dis.readFully(bytes);
                        values.add(new String(bytes));
                    }
                }
            }
        }
        
        return new ColumnFileData(type, values);
    }

    // Function to detect column type (Integer, Float, String)
    public static String detectColumnType(List<String> columnData) {
        boolean isInteger = true;
        boolean isFloat = true;

        for (String value : columnData) {
            if (!value.matches("-?\\d+")) {
                isInteger = false;
            }
            if (!value.matches("-?\\d+(\\.\\d+)?")) {
                isFloat = false;
            }
        }

        if (isInteger) return "Integer";
        if (isFloat) return "Float";
        return "String";
    }

    // Function to write data to existing .col file
    public static void writeToColFile(String filename, List<String> data, String columnType) {
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename))) {
            dos.writeUTF(columnType); // Store column type at the beginning
            dos.writeInt(data.size()); // Store row count

            for (String value : data) {
                switch (columnType) {
                    case "Integer" -> dos.writeInt(Integer.parseInt(value)); // Store as Integer (4 bytes)
                    case "Float" -> dos.writeFloat(Float.parseFloat(value)); // Store as Float (4 bytes)
                    case "String" -> {
                        byte[] bytes = value.getBytes();
                        dos.writeInt(bytes.length); // Store string length
                        dos.write(bytes); // Store actual string bytes
                    }
                }
            }
            System.out.println("Updated column (" + columnType + ") in " + filename);
        } catch (IOException e) {
            System.out.println("Error writing to .col file: " + filename);
            e.printStackTrace();
        }
    }
}
