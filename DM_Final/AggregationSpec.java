import java.util.List;

/**
 * Represents an aggregation specification for a fact column
 */
public class AggregationSpec {
    private String factColumn;
    private List<String> operations;
    
    public AggregationSpec(String factColumn, List<String> operations) {
        this.factColumn = factColumn;
        this.operations = operations;
    }
    
    public String getFactColumn() {
        return factColumn;
    }
    
    public List<String> getOperations() {
        return operations;
    }
}
