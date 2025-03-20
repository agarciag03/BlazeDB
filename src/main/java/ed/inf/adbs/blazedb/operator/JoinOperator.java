package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class JoinOperator extends Operator{
    private Operator leftChild;
    private Operator rightChild;
    private Expression joinCondition;
    private Tuple leftTuple;
    private Tuple rightTuple;

    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;
        this.leftTuple = null;
        this.rightTuple = null;
    }

    @Override
    public Tuple getNextTuple() throws Exception {
        if (leftTuple == null) {
            leftTuple = leftChild.getNextTuple(); // Get the first tuple from the left child
        }

        // the join should scan the left (outer) child once, and for each tuple in the outer child, it should scan the inner child
        //completely (finally a use for the reset() method
        while (leftTuple != null) {
            rightTuple = rightChild.getNextTuple();
            while (rightTuple != null) { // Cross product
                if (joinCondition == null) {
                    // cross product
                    return leftTuple.join(rightTuple);
                } else if (evaluateJoinCondition(leftTuple, rightTuple)) { // If join condition, the tuple is only returned if it matches the join condition
                    return leftTuple.join(rightTuple);
                }
                // if they dont match, get the next right tuple
                rightTuple = rightChild.getNextTuple();
            }
            // when right tuple is null, get the next left tuple and start right child again
            rightChild.reset();
            leftTuple = leftChild.getNextTuple();
        }
        return null;
    }

    @Override
    public void reset() throws Exception {
        leftChild.reset();
        rightChild.reset();
        leftTuple = null;
        rightTuple = null;
    }

    private boolean evaluateJoinCondition(Tuple leftTuple, Tuple rightTuple) {
        ConditionEvaluator conditionEvaluator = new ConditionEvaluator(joinCondition);
        return conditionEvaluator.evaluateJoin(leftTuple, rightTuple);
    }
}
