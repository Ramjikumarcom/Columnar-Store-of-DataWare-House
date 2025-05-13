import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class ReadColFileBinary {
    public static void main(String[] args) {
        String columnFilename = "marketdb\\fact_table\\Customer_Segment.col";

        try (DataInputStream dis = new DataInputStream(new FileInputStream(columnFilename))) {
            String columnType = dis.readUTF(); // Read stored data type
            int rowCount = dis.readInt(); // Read total rows

            

            for (int i = 0; i < rowCount; i++) {
                switch (columnType) {
                    case "Integer":
                        System.out.println(dis.readInt()); // Read Integer
                        break;
                    case "Float":
                        System.out.println(dis.readFloat()); // Read Float
                        break;
                    case "String":
                        int length = dis.readInt(); // Read string length
                        byte[] bytes = new byte[length];
                        dis.readFully(bytes); // Read actual string
                        System.out.println(new String(bytes)); // Convert bytes to string
                        break;
                }
            }
            System.out.println("Column Type: " + columnType);
            System.out.println("Total Rows: " + rowCount);
        } catch (IOException e) {
            System.out.println("Error reading .col file.");
            e.printStackTrace();
        }
    }
}
