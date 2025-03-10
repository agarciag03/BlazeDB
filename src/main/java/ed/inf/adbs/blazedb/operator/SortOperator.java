package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;


public class SortOperator extends Operator{
    private Operator child;
    private List<String> orderByColumns;
    private List<Tuple> SortedTuples; //Internal Buffer
    private Iterator<Tuple> iteratorSortedTuples; //Iterator internal Buffer


    public SortOperator(Operator child, List<OrderByElement> orderByColumn) throws Exception {
        this.child = child;
        this.orderByColumns = getColumnsOrderBy(orderByColumn);
    }

    private List<String> getColumnsOrderBy(List<OrderByElement> orderByColumn) {
        List<String> columns = new ArrayList<>();
        for (OrderByElement element : orderByColumn) {
            Column column = (Column) element.getExpression();
            columns.add(column.toString());
        }
        return columns;
    }

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
                for (String column : orderByColumns) {
                    int columnIndex = tuple1.getColumnIndex(column);
                    int comparison = tuple1.getValue(columnIndex).compareTo(tuple2.getValue(columnIndex));

                    if (comparison != 0) { // Si hay diferencia, devolvemos el resultado
                        return comparison;
                    }
                }
                return 0; // if the tuples are equal in all columns, they are considered equal
            }
        });
        return tuples;
    }

    @Override
    public Tuple getNextTuple() throws Exception {

        // blocking operator to sort all the tuples
        if (iteratorSortedTuples == null) {
            this.SortedTuples = sortTuples(getAllTuples(child));
            this.iteratorSortedTuples = SortedTuples.iterator();
        }

        // send the next tuple
        if (iteratorSortedTuples.hasNext()) {
            return iteratorSortedTuples.next();
        }
        return null;
    }

    @Override
    public void reset() throws Exception {
        //Scan all the sorted list again
        this.iteratorSortedTuples = SortedTuples.iterator();
    }
}
