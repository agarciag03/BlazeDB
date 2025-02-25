package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends Operator {

    private Operator child; // This operator has one child
    private Expression condition; // The condition that needs to be satisfied
    private ConditionEvaluator evaluator; // Evaluator to check the condition

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

    // it has a child so, it should reset the child
    @Override
    public void reset() throws Exception {
        child.reset();
    }

}

