package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * This class is responsible for selecting tuples from the result of a query based on a specified condition.
 * The getNextTuple method is used to get the next tuple from the child operator and evaluate the condition.
 */
public class SelectOperator extends Operator {

    private Operator child;
    private ConditionEvaluator evaluator;

    /**
     * Constructor to initialize the select operator.
     * @param child Child operator to get the tuples
     * @param condition Condition to evaluate the tuples
     */
    public SelectOperator(Operator child, Expression condition) {
        this.child = child;
        this.evaluator = new ConditionEvaluator(condition);
    }

    /**
     * Get the next tuple from the child operator and evaluate the condition.
     * @return Tuples that satisfy the given condition
     * @throws Exception If the child operator throws an exception
     */
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

    /**
     * Reset the child operator to read the tuples from the beginning.
     * @throws Exception If the child operator throws an exception
     */
    @Override
    public void reset() throws Exception {
        child.reset();
    }

}

