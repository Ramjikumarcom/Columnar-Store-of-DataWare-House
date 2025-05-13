import java.io.*;
import java.util.*;

/**
 * Utility class for file operations
 */
public class FileUtils {
    
    /**
     * Reads a binary column file and returns its contents as a list of strings
     * 
     * @param filePath Path to the binary column file
     * @return List of string values from the file
     */
    public static List<String> readBinaryColFile(String filePath) {
        List<String> values = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new FileInputStream(filePath))) {
            String columnType = dis.readUTF();
            int rowCount = dis.readInt();
            
            for (int i = 0; i < rowCount; i++) {
                switch (columnType) {
                    case "Integer":
                        values.add(String.valueOf(dis.readInt()));
                        break;
                    case "Float":
                        values.add(String.valueOf(dis.readFloat()));
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
        } catch (IOException e) {
            System.err.println("Error reading binary col file: " + filePath);
            e.printStackTrace();
        }
        return values;
    }
    
    /**
     * Checks if a cuboid file exists
     * 
     * @param dimensions List of dimensions that define the cuboid
     * @param cuboidDir Directory where cuboid files are stored
     * @return true if the file exists, false otherwise
     */
    public static boolean cuboidFileExists(List<String> dimensions, String cuboidDir) {
        String fileName = dimensions.isEmpty() ? "base_cuboid.csv" : 
                         String.join("_", dimensions).replaceAll("\\s+", "_") + ".csv";
        File file = new File(cuboidDir + fileName);
        return file.exists();
    }
}
