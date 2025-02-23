package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter; // review the visitorAdapter

public class SelectOperator extends Operator {
    private Operator child; // This operator has one child
    private Expression condition; // The condition that needs to be satisfied
    //private ExpressionEvaluator evaluator; // review

    public SelectOperator(Operator child, Expression condition) {
        this.child = child;
        this.condition = condition;
        //this.evaluator = new ExpressionEvaluator(); // Review it
    }

    @Override
    public Tuple getNextTuple() throws Exception {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
//            if (evaluator.evaluate(condition, tuple)) {
//                return tuple;
//            }
        }
        return null;
    }

    // it has a child so, it should reset the child
    @Override
    public void reset() throws Exception {
        child.reset();
    }

}

