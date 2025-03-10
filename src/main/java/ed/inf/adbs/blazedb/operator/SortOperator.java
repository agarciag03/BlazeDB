package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class SortOperator extends Operator{
    private Operator child;
    private List<Integer> orderByColumns;
    private Integer orderByColumn;
    private String orderByCol;
    private List<Tuple> sortedTuples; //Internal Buffer
    private int index;


    //new implementation
    public SortOperator(Operator child, Expression orderByColumn) throws Exception {
        this.child = child;
        //this.orderByColumn = identifyColumnIndex(orderByColumn);
        this.orderByCol = ((Column) orderByColumn).toString();
        this.sortedTuples = sortTuples(getAllTuples(child)); // Read all of the output from its child operator and sort it.
        this.index = 0;
    }

//    private Integer identifyColumnIndex(Expression orderByColumn) {
//        if (orderByColumn != null) {
//            Column column = (Column) orderByColumn;
//            String tableName = column.getTable().getName();
//            String columnName = column.getColumnName();
//            int columnIndex = Catalog.getInstance().getColumnIndex(tableName, columnName);
//            return columnIndex;
//        }
//        return null;
//    }

//    public SortOperator(Operator child, List<Integer> orderByColumns) throws Exception {
//        this.child = child;
//        this.orderByColumns = orderByColumns;
//        this.sortedTuples = sortTuples(getAllTuples(child)); // Read all of the output from its child operator and sort it.
//        this.index = 0;
//    }

    public List <Tuple> getAllTuples(Operator child) throws Exception {
        List <Tuple> tuples = new ArrayList<>();
        Tuple tuple;

        while ((tuple = child.getNextTuple()) != null) {
            tuples.add(tuple);
        }
        return tuples;
    }

    public List<Tuple> sortTuples(List<Tuple> tuples) {
        Collections.sort(tuples, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple tuple1, Tuple tuple2) {
                //int columnIndex = orderByColumns.get(0);
                int orderByColumn = tuple1.getColumnIndex(orderByCol);
                return tuple1.getValue(orderByColumn).compareTo(tuple2.getValue(orderByColumn));
                //return t1.getValue(columnIndex).compareTo(t2.getValue(columnIndex));
            }
        });
        return tuples;
//        Collections.sort(tuples, new Comparator<Tuple>() {
//            @Override
//            public int compare(Tuple t1, Tuple t2) {
//                for (String column : orderByColumns) {
//                    int comparison = t1.getValue(column).compareTo(t2.getValue(column));
//                    if (comparison != 0) {
//                        return comparison;
//                    }
//                }
//                return 0;
//            }
//        });
        //return null; //tuples;
    }


    @Override
    public Tuple getNextTuple() throws Exception {
        // Return the next tuple from the sorted list
        if (index < sortedTuples.size()) {
            return sortedTuples.get(index++);
        }
        return null;
    }

    @Override
    public void reset() throws Exception {
        //Scan all the sorted list again
        index = 0;
    }
}
