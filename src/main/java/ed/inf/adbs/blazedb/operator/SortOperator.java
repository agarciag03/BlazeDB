package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import java.util.*;

/**
 * SortOperator class is used to sort the tuples based on the columns specified in the orderByColumns.
 * CompareTo method is used to sort the tuples in ascending order.
 */
public class SortOperator extends Operator{
    private Operator child;
    private List<OrderByElement> orderByExpressions;
    // Buffers for sorting the tuples
    private List<Tuple> SortedTuples; //Internal Buffer
    private Iterator<Tuple> iteratorSortedTuples; //Iterator internal Buffer

    /**
     * SortOperator constructor
     * @param child Operator
     * @param orderByColumn a list of columns to sort by
     * @throws Exception
     */
    public SortOperator(Operator child, List<OrderByElement> orderByColumn) throws Exception {
        this.child = child;
        this.orderByExpressions = orderByColumn;
    }

    /**
     * This method Get the next tuple from a list of sorted tuples (Blocking operator) after getting all the tuples from the child operator and
     * sort them. If the list of sorted tuples is empty, it will get all the tuples from the child operator and sort them.
     * @return Next tuple of the sorted list based on the tuples of the child operator
     * @throws Exception
     */
    @Override
    public Tuple getNextTuple() throws Exception {

        // blocking operator to sort all the tuples
        if (iteratorSortedTuples == null) {
            this.SortedTuples = sortTuples(getAllTuples(child));
            this.iteratorSortedTuples = SortedTuples.iterator();
        }
        // When all the tuples are in the iteratorSortedTuples, the program just sends the next tuple
        if (iteratorSortedTuples.hasNext()) {
            return iteratorSortedTuples.next();
        }
        return null;
    }

    /**
     * Get all the tuples from the child operator. This operator is a blocking operator.
     * @param child  the child operator
     * @return List<Tuple> a list of all the tuples from the child operator
     * @throws Exception
     */
    public List <Tuple> getAllTuples(Operator child) throws Exception {
        List <Tuple> tuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            tuples.add(tuple);
        }
        return tuples;
    }

    /**
     * Sort the tuples based on the columns specified in the orderByColumns. This method compares the values of the
     * tuples. If the values are equal, the next column is compared. If all the values are equal, the tuples are considered equal.
     * @param tuples a list of tuples to sort
     * @return List<Tuple> a list of sorted tuples in ascending order
     */
    public List<Tuple> sortTuples(List<Tuple> tuples) {
        Collections.sort(tuples, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple tuple1, Tuple tuple2) {
                for (OrderByElement orderByColumn : orderByExpressions) {
                    Column column = (Column) orderByColumn.getExpression();
                    // Compare the values of the tuples in the column
                    int comparison = tuple1.getValue(column.toString()).compareTo(tuple2.getValue(column.toString()));
                    if (comparison != 0) {
                        // If there is a difference, return the result
                        return comparison;
                    }
                }
                return 0; // if the tuples are equal in all columns, they are considered equal
            }
        });
        return tuples;
    }

    /**
     * Reset the operator to the beginning of the sorted list. It is applied when the sorted list is needed again
     * Therefore, the iteratorSortedTuples is set again with the sorted list.
     * @throws Exception
     */
    @Override
    public void reset() throws Exception {
        //Scan all the sorted list again
        this.iteratorSortedTuples = SortedTuples.iterator();
    }
}
