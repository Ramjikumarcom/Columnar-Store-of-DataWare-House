import java.io.File;
import java.util.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Utility class for XML operations
 */
public class XmlUtils {
    
    /**
     * Parses lattice dimensions from an XML file
     * 
     * @param xmlFilePath Path to the XML file
     * @return List of lattice dimensions
     * @throws Exception if parsing fails
     */
    public static List<String> parseLatticeDimensions(String xmlFilePath) throws Exception {
        List<String> dimensions = new ArrayList<>();
        
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new File(xmlFilePath));
        
        NodeList nodeList = document.getElementsByTagName("LatticeDimension");
        
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                dimensions.add(node.getTextContent().trim());
            }
        }
        
        return dimensions;
    }
    
    /**
     * Parses aggregation specifications from an XML file
     * 
     * @param xmlFilePath Path to the XML file
     * @return List of aggregation specifications
     * @throws Exception if parsing fails
     */
    public static List<AggregationSpec> parseAggregationSpecs(String xmlFilePath) throws Exception {
        List<AggregationSpec> specs = new ArrayList<>();
        
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new File(xmlFilePath));
        
        NodeList cuboidAggrNodeList = document.getElementsByTagName("CuboidAggregations");
        if (cuboidAggrNodeList.getLength() == 0) {
            return getDefaultAggregationSpecs();
        }
        
        Node cuboidAggrNode = cuboidAggrNodeList.item(0);
        NodeList factAggregationNodes = ((Element) cuboidAggrNode)
                                       .getElementsByTagName("FactAggregation");
        
        for (int i = 0; i < factAggregationNodes.getLength(); i++) {
            Node factAggrNode = factAggregationNodes.item(i);
            
            if (factAggrNode.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) factAggrNode;
                String factColumn = element.getAttribute("column");
                
                if (factColumn != null && !factColumn.isEmpty()) {
                    NodeList aggregationNodes = element.getElementsByTagName("Aggregation");
                    List<String> operations = new ArrayList<>();
                    
                    for (int j = 0; j < aggregationNodes.getLength(); j++) {
                        String operation = aggregationNodes.item(j).getTextContent().trim().toUpperCase();
                        operations.add(operation);
                    }
                    
                    if (!operations.isEmpty()) {
                        specs.add(new AggregationSpec(factColumn, operations));
                        System.out.println("Added aggregation spec for " + factColumn + ": " + 
                                          String.join(", ", operations));
                    }
                }
            }
        }
        
        if (specs.isEmpty()) {
            return getDefaultAggregationSpecs();
        }
        
        return specs;
    }
    
    /**
     * Gets default aggregation specifications
     * 
     * @return List of default aggregation specifications
     */
    private static List<AggregationSpec> getDefaultAggregationSpecs() {
        List<AggregationSpec> defaultSpecs = new ArrayList<>();
        
        List<String> defaultOps = Arrays.asList("SUM");
        defaultSpecs.add(new AggregationSpec("Sales", defaultOps));
        defaultSpecs.add(new AggregationSpec("Profit", defaultOps));
        defaultSpecs.add(new AggregationSpec("Discount", defaultOps));
        defaultSpecs.add(new AggregationSpec("Order_Quantity", defaultOps));
        defaultSpecs.add(new AggregationSpec("Shipping_Cost", defaultOps));
        defaultSpecs.add(new AggregationSpec("Product_Base_Margin", defaultOps));
        
        System.out.println("Using default aggregations (SUM) for all fact measures");
        return defaultSpecs;
    }
}
