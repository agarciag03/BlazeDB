package ed.inf.adbs.blazedb;

import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;


/**
 * The Tuple class represents a row of data.
 *
 * You will need to modify this class, obviously :).
 */
public class Tuple {
    private List<Integer> values;

    // Constructor
    public Tuple (String line){
        values = new ArrayList<>();
        for (String value : line.split(",")){
            values.add(Integer.parseInt(value.trim())); //trim() removes whitespaces from the beginning and end of a string
        }
    }

    // This methods allows to get a particular value based on the index
    public Integer getValue(int index) {
        return values.get(index);
    }

//    //this method allows to get a particular value based on the column name
//    public Integer getValue(String columnName) {
//
//        return values.get(index);
//    }

    //This method returns the tuples values split by a comma as expected output shows
    @Override
    public String toString() {
        return values.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }
}