package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.operator.Operator;

import java.util.List;
import java.util.Arrays;

public class DummyOperator extends Operator {
    private List<Tuple> tuples;
    private int index;

    public DummyOperator(List<Tuple> tuples) {
        this.tuples = tuples;
        this.index = 0;
    }

    @Override
    public Tuple getNextTuple() {
        if (index < tuples.size()) {
            return tuples.get(index++);
        }
        return null;
    }

    @Override
    public void reset() {
        this.index = 0;
    }
}
