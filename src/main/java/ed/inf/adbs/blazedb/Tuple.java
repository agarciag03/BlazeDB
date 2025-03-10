package ed.inf.adbs.blazedb;

import java.util.*;
import java.util.stream.Collectors;


/**
 * The Tuple class represents a row of data.
 */

public class Tuple {
    private List<String> columnNames = new ArrayList<>();
    private List<Integer> values;

    // Constructor #1 of tuples
    public Tuple (String line){
        values = new ArrayList<>();
        for (String value : line.split(",")){
            values.add(Integer.parseInt(value.trim())); //trim() removes whitespaces from the beginning and end of a string
        }
    }

    // Constructor #2 of tuples with names
    public Tuple (String line, String tableName, String[] columnNames){

        // Capture the values of the tuple
        values = new ArrayList<>();
        for (String value : line.split(",")){
            values.add(Integer.parseInt(value.trim())); //trim() removes whitespaces from the beginning and end of a string
        }

        for (String columnName : columnNames) {
            this.columnNames.add(tableName + "." + columnName);
        }

        if (values.size() != columnNames.length) {
            System.out.println("Error: Tuple values and column names do not match");
        }

    }

    // Constructor #3
    public Tuple() {
        values = new ArrayList<>();
    }

    // This methods allows to get a particular value based on the index
    public Integer getValue(int index) {
        return values.get(index);
    }

    public List<Integer> getValues() {
        return values;
    }

    public void addValue(int value, String columnName) {
        values.add(value);
        columnNames.add(columnName);
    }

    public void addValues(List<Integer> values, List<String> columnNames) {
        this.values.addAll(values);
        this.columnNames.addAll(columnNames);
    }

    public Tuple join(Tuple tuple) {

        // Join the tuples values
        Tuple joinedTuple = new Tuple();
        joinedTuple.addValues(this.values, this.columnNames);
        joinedTuple.addValues(tuple.getValues(), tuple.columnNames);

        //joinedTuple.printTupleWithColumns();

        return joinedTuple;
    }

    //This method returns the tuples values split by a comma as expected output shows
    @Override
    public String toString() {
        return values.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    public void printTupleWithColumns() {
        String plainTuple = "Columns: " + columnNames.toString() + " values: " + this.toString();
        System.out.println(plainTuple);
        }

    // Adjusting the hashing function to return unique values
    @Override
    public int hashCode() {
       return Objects.hash(values);
    }

    //Overwrite to compare the values of the tuples for hashing
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(values, tuple.values);
    }

    public int getColumnIndex(String columnName) {
        return columnNames.indexOf(columnName);
    }

}