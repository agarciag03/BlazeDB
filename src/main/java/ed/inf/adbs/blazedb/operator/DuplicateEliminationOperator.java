package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

import java.util.HashSet;
import java.util.Set;

public class DuplicateEliminationOperator extends Operator {
    private Operator child;
    private Set<Tuple> seenTuples; // Set doesn't allow duplicates

    public DuplicateEliminationOperator(Operator child) {
        this.child = child;
        this.seenTuples = new HashSet<>();
    }

    @Override
    public Tuple getNextTuple() throws Exception {
        Tuple tuple = child.getNextTuple();
        while (tuple != null){
            if(seenTuples.add(tuple)){ // If the tuple is not already in the set
                return tuple;
            }
            tuple = child.getNextTuple();
        }
        return null;

    }

    @Override
    public void reset() throws Exception {
        child.reset();
        seenTuples.clear();
    }
}
