/**
 * Represents a filter condition for querying cuboid data
 * Used to filter rows based on column values using different comparison operators
 */
public class FilterCondition {
    int columnIndex;    // Zero-based index of the column to apply the filter on
    String operator;    // Comparison operator (=, !=, >, <)
    String filterValue; // Value to compare against
    
    /**
     * Creates a new filter condition
     * 
     * @param columnIndex Zero-based index of the column to filter
     * @param operator Comparison operator (=, !=, >, <)
     * @param filterValue Value to compare against
     */
    public FilterCondition(int columnIndex, String operator, String filterValue) {
        this.columnIndex = columnIndex;
        this.operator = operator;
        this.filterValue = filterValue;
    }
    
    /**
     * Determines if a data row matches this filter condition
     * Handles both numeric and string comparisons appropriately
     * 
     * @param values Array of cell values from a data row
     * @return true if the row matches the condition, false otherwise
     */
    public boolean matches(String[] values) {
        if (columnIndex >= values.length) return false;
        
        String cellValue = values[columnIndex];
        
        switch (operator) {
            case "=":
                return cellValue.equals(filterValue);
            case "!=":
                return !cellValue.equals(filterValue);
            case ">":
                try {
                    // Try numeric comparison first
                    double cellNum = Double.parseDouble(cellValue);
                    double filterNum = Double.parseDouble(filterValue);
                    return cellNum > filterNum;
                } catch (NumberFormatException e) {
                    // Fall back to string comparison if not numeric
                    return cellValue.compareTo(filterValue) > 0;
                }
            case "<":
                try {
                    // Try numeric comparison first
                    double cellNum = Double.parseDouble(cellValue);
                    double filterNum = Double.parseDouble(filterValue);
                    return cellNum < filterNum;
                } catch (NumberFormatException e) {
                    // Fall back to string comparison if not numeric
                    return cellValue.compareTo(filterValue) < 0;
                }
            default:
                return false;
        }
    }
}
