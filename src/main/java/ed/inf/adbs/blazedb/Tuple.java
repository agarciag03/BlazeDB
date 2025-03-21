package ed.inf.adbs.blazedb;

import java.util.*;
import java.util.stream.Collectors;


/**
 * The Tuple class represents a row of data containing values and column names.
 * Some of the methods in this class are used to manipulate the tuples.
 */

public class Tuple {
    private List<String> columnNames = new ArrayList<>();
    private List<Integer> values;

    /**
     * Default constructor that initializes an empty tuple.
     */
    public Tuple() {
        values = new ArrayList<>();
    }

    /**
     * Constructor that initializes a tuple with values and column names.
     * The column names are prefixed with the table name. This constructor is used in the ScanOperator class
     * as a way by default to create tuples with column names.
     *
     * @param line The line of data to be split by commas
     * @param tableName The name of the table to prefix column names
     * @param columnNames The column names associated with the values
     */

    public Tuple (String line, String tableName, String[] columnNames){

        // Capture the values of the tuple and transform them into integer list
        values = new ArrayList<>();
        for (String value : line.split(",")){
            values.add(Integer.parseInt(value.trim())); //trim() removes whitespaces from the beginning and end of a string
        }

        // Capture the column names of the tuple and prefix them with the table name
        for (String columnName : columnNames) {
            this.columnNames.add(tableName + "." + columnName);
        }

        // Check if the number of values and column names match
        if (values.size() != columnNames.length) {
            System.out.println("Error: Tuple values and column names do not match");
        }

    }

    /**
     * Retrieves a value from the columnName.
     *
     * @param columnName The name of the column to retrieve the value from
     * @return The integer value at the specified columnName
     */
    public Integer getValue(String columnName) {
        return values.get(columnNames.indexOf(columnName));
    }

    /**
     * Returns all values in the tuple. It is used in the join operation.
     *
     * @return A list of integer values
     */
    public List<Integer> getValues() {
        return values;
    }

    /**
     * Adds a value to the tuple with its corresponding column name. It is used in projection.
     *
     * @param value The integer value to be added
     * @param columnName The name of the column to add the value to
     */
    public void addValue(int value, String columnName) {
        values.add(value);
        columnNames.add(columnName);
    }

    /**
     * Adds a list of values to the tuple with their corresponding column names. It is mainly used in the join operation.
     *
     * @param values The list of integer values to be added
     * @param columnNames The list of column names to add the values to
     */
    public void addValues(List<Integer> values, List<String> columnNames) {
        this.values.addAll(values);
        this.columnNames.addAll(columnNames);
    }

    /**
     * Joins two tuples together. It is used in the join operation.
     * The method concatenates the values and column names of the two tuples.
     *
     * @param tuple The tuple to be joined with
     * @return The joined tuple
     */
    public Tuple join(Tuple tuple) {

        // Create a new tuple and add the values and column names of the two tuples
        Tuple joinedTuple = new Tuple();
        joinedTuple.addValues(this.values, this.columnNames);
        joinedTuple.addValues(tuple.getValues(), tuple.columnNames);

        return joinedTuple;
    }

    /**
     * Returns the values of the tuple as a string separated by commas.
     *
     * @return A string of values separated by commas
     */
    @Override
    public String toString() {
        return values.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    /**
     * Prints the tuple with its column names. It is mainly used for debugging purposes.
     */
    public void printTupleWithColumns() {
        String plainTuple = "Columns: " + columnNames.toString() + " values: " + this.toString();
        System.out.println(plainTuple);
        }

    /**
     * Returns a hash code value for the object. This method is supported for the benefit of hash tables such as those provided by HashMap.
     * The hash code is based on the values of the tuple. It is done to ensure that tuples with the same values have the same hash code.
     *
     * @return A hash code value for this object
     */
    @Override
    public int hashCode() {
       return Objects.hash(values);
    }

    /**
     * Compares this tuple to another object for equality. It is mainly used in group by and sum operations.
     * Where the tuples are grouped based on their values.
     *
     * @param o The object to compare with this tuple
     * @return True if the tuples have identical values, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(values, tuple.values);
    }

}