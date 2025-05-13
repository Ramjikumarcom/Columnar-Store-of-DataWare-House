import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;

public class DWHStructureGenerator {
    
    public static void main(String[] args) {
        String xmlFilePath = "/home/ramji/Desktop/DM_Final/newmarket.xml";
        
        try {
            parseXmlAndCreateStructure(xmlFilePath);
        } catch (Exception e) {
            System.err.println("Error processing XML file: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static void parseXmlAndCreateStructure(String xmlFilePath) throws Exception {
        // Load and parse the XML file
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new File(xmlFilePath));
        document.getDocumentElement().normalize();
        
        // Get the warehouse name for the base folder
        Element root = document.getDocumentElement();
        String warehouseName = root.getAttribute("Name");
        if (warehouseName.isEmpty()) {
            warehouseName = "datawarehouse";
        }
        
        // Create base directory path
        String xmlDirectory = new File(xmlFilePath).getParent();
        String baseDir = xmlDirectory + File.separator + warehouseName;
        
        // Create base directory if it doesn't exist
        File baseDirFile = new File(baseDir);
        if (!baseDirFile.exists()) {
            baseDirFile.mkdir();
            System.out.println("Created base directory: " + baseDir);
        }
        
        // Process dimension tables
        NodeList dimTablesList = document.getElementsByTagName("DimensionTablesList");
        if (dimTablesList.getLength() > 0) {
            NodeList dimTables = ((Element) dimTablesList.item(0)).getElementsByTagName("DimensionTable");
            System.out.println("Found " + dimTables.getLength() + " dimension tables.");
            
            for (int i = 0; i < dimTables.getLength(); i++) {
                Element dimTable = (Element) dimTables.item(i);
                String tableName = dimTable.getAttribute("Name");
                if (tableName.isEmpty()) {
                    tableName = "unknown_table_" + i;
                }
                
                String tableDir = baseDir + File.separator + tableName;
                File tableDirFile = new File(tableDir);
                if (!tableDirFile.exists()) {
                    tableDirFile.mkdir();
                    System.out.println("Created directory for " + tableName + ": " + tableDir);
                }
                
                // Process attributes
                NodeList attributes = dimTable.getElementsByTagName("DAttr");
                System.out.println("  Found " + attributes.getLength() + " attributes in table " + tableName);
                
                for (int j = 0; j < attributes.getLength(); j++) {
                    Element attr = (Element) attributes.item(j);
                    String attrName = attr.getAttribute("name");
                    String attrType = attr.getAttribute("type");
                    
                    if (attrName.isEmpty()) {
                        attrName = "unknown_attr_" + j;
                    }
                    
                    String colFilePath = tableDir + File.separator + attrName + ".col";
                    try (FileWriter writer = new FileWriter(colFilePath)) {
                        // Creating empty file with no content
                        System.out.println("    Created empty file: " + colFilePath);
                    }
                }
            }
        } else {
            System.out.println("No dimension tables found in the XML file.");
        }
        
        // Process fact table
        NodeList factTables = document.getElementsByTagName("FactTable");
        if (factTables.getLength() > 0) {
            Element factTable = (Element) factTables.item(0);
            String tableName = factTable.getAttribute("Name");
            if (tableName.isEmpty()) {
                tableName = "fact";
            }
            
            String tableDir = baseDir + File.separator + tableName + "_table";
            File tableDirFile = new File(tableDir);
            if (!tableDirFile.exists()) {
                tableDirFile.mkdir();
                System.out.println("Created directory for fact table: " + tableDir);
            }
            
            // Process foreign keys - create directly in fact table directory
            NodeList fkeysNodes = factTable.getElementsByTagName("FKeys");
            if (fkeysNodes.getLength() > 0) {
                Element fkeysElem = (Element) fkeysNodes.item(0);
                NodeList fkeys = fkeysElem.getElementsByTagName("FAttr");
                
                for (int i = 0; i < fkeys.getLength(); i++) {
                    Element fkey = (Element) fkeys.item(i);
                    String keyName = fkey.getAttribute("name");
                    String keyType = fkey.getAttribute("type");
                    
                    String colFilePath = tableDir + File.separator + keyName + ".col";
                    try (FileWriter writer = new FileWriter(colFilePath)) {
                        // Creating empty file with no content
                        System.out.println("    Created empty foreign key file: " + colFilePath);
                    }
                }
            }
            
            // Process fact variables - create directly in fact table directory
            NodeList factVarsNodes = factTable.getElementsByTagName("FactVariablesList");
            if (factVarsNodes.getLength() > 0) {
                Element factVarsElem = (Element) factVarsNodes.item(0);
                NodeList factVars = factVarsElem.getElementsByTagName("FAttr");
                
                for (int i = 0; i < factVars.getLength(); i++) {
                    Element factVar = (Element) factVars.item(i);
                    String varName = factVar.getAttribute("name");
                    String varType = factVar.getAttribute("type");
                    
                    String colFilePath = tableDir + File.separator + varName + ".col";
                    try (FileWriter writer = new FileWriter(colFilePath)) {
                        // Creating empty file with no content
                        System.out.println("    Created empty measure file: " + colFilePath);
                    }
                }
            }
        }
        
        System.out.println("Processing complete!");
    }
}