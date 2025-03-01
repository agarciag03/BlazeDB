package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortOperator extends Operator{
    private List<Tuple> sortedTuples; //Internal Buffer
    private int index;

    public SortOperator(Operator child, List<String> orderByColumns) throws Exception {
        //Read all tuples from its child
        List <Tuple> tuples = getAllTuples(child);

        //Sort the tuples based on the order by columns
        // Sort the tuples using a custom comparator
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

//        this.sortedTuples = tuples;
//        this.index = 0;
    }

    public List <Tuple> getAllTuples(Operator child) throws Exception {
        List <Tuple> tuples = new ArrayList<>();
        Tuple tuple;

        while ((tuple = child.getNextTuple()) != null) {
            tuples.add(tuple);
        }
        return tuples;
    }

    @Override
    public Tuple getNextTuple() throws Exception {
//        if (index < sortedTuples.size()) {
//            return sortedTuples.get(index++);
//        }
        return null;
    }

    @Override
    public void reset() throws Exception {
        //index = 0;
    }
}
