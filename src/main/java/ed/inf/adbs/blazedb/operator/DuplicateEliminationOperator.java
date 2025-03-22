package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is responsible for removing duplicate tuples from the result of a query.
 * It uses a HashSet to store the tuples that have been seen.
 * The getNextTuple method is used to get the next tuple from the child operator and remove duplicates.
 */
public class DuplicateEliminationOperator extends Operator {
    private Operator child;
    private Set<Tuple> seenTuples; // HashSet is used to avoid duplicates

    /**
     * Constructor for the DuplicateEliminationOperator class.
     * @param child The child operator.
     *
     */
    public DuplicateEliminationOperator(Operator child) {
        this.child = child;
        this.seenTuples = new HashSet<>();
    }

    /**
     * Get the next tuple from the child operator, if the tuple is not already in the set of seen tuples, return it.
     * Otherwise, get the next tuple from the child operator.
     * @return the next tuple from the child operator without duplicates.
     * @throws Exception
     */
    @Override
    public Tuple getNextTuple() throws Exception {
        Tuple tuple = child.getNextTuple();
        while (tuple != null){
            if(seenTuples.add(tuple)){ // If the tuple is not already in the set
                return tuple;
            }
            tuple = child.getNextTuple(); // Otherwise, get the next tuple
        }
        return null;
    }

    /**
     * Reset the child operator and the set of seen tuples.
     * @throws Exception
     */
    @Override
    public void reset() throws Exception {
        child.reset();
        seenTuples.clear();
    }
}
