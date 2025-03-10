package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends Operator {

    private Operator child;
    private Expression condition;
    private ConditionEvaluator evaluator;

    public SelectOperator(Operator child, Expression condition) {
        this.child = child;
        this.condition = condition;
        this.evaluator = new ConditionEvaluator(condition);
    }

    @Override
    public Tuple getNextTuple() throws Exception {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) { // While Child has tuples
            if (evaluator.evaluate(tuple)) { // Evaluate the given condition
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset() throws Exception {
        child.reset();
    }

}

