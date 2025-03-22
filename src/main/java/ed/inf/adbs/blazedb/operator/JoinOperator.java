package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * The JoinOperator class represents a join operation between two child operators
 * (left and right) based on a specified join condition.
 * It performs a nested loop join where the left child (outer) is scanned once,
 * and for each tuple from the left, the right child (inner) is completely scanned.
 * If a join condition is provided, tuples are only returned if they satisfy the condition.
 * If no join condition is provided, a cross product (Cartesian product) of the tuples is returned.
 */
public class JoinOperator extends Operator{
    private Operator leftChild;
    private Operator rightChild;
    private Expression joinCondition;
    private ConditionEvaluator evaluator;
    private Tuple leftTuple;
    private Tuple rightTuple;

    /**
     * Constructor for the JoinOperator class.
     * @param leftChild The left child operator, which is always the root.
     * @param rightChild The right child operator.
     * @param joinCondition The join condition.
     */
    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;
        this.evaluator = new ConditionEvaluator(joinCondition);
        this.leftTuple = null;
        this.rightTuple = null;
    }

    /**
     * In this method, the join operation is performed. The algorithm applied is a nested loop join and it works as follows:
     * Get the next tuple from the left child and scan the right child for each tuple from the left child.
     * If a join condition is provided, return the tuple only if it satisfies the condition.
     * If no join condition is provided, return the cross product of the tuples.
     * @return The joined tuple for the next operation. If no more tuples are available, return null.
     * @throws Exception
     */
    @Override
    public Tuple getNextTuple() throws Exception {

        if (leftTuple == null) {
            // Get the first tuple from the left child
            leftTuple = leftChild.getNextTuple();
        }

        // the join should scan the left (outer) child once, and for each tuple it should scan the inner child
        while (leftTuple != null) {
            rightTuple = rightChild.getNextTuple();
            while (rightTuple != null) {

                if (joinCondition == null) {
                    // When no join condition is provided, return the cross product of the tuples
                    return leftTuple.join(rightTuple);

                } else if (evaluateJoinCondition(leftTuple, rightTuple)) {
                    // If join condition, the tuple is only returned if it matches the join condition
                    return leftTuple.join(rightTuple);
                }
                // if the condition is not matched, get the next right tuple
                rightTuple = rightChild.getNextTuple();
            }

            // when right tuple is null, get the next left tuple and start right child again
            leftTuple = leftChild.getNextTuple();
            rightChild.reset();
        }
        return null;
    }

    /**
     * Reset the left and right child operators and the left and right tuples.
     * @throws Exception
     */
    @Override
    public void reset() throws Exception {
        leftChild.reset();
        rightChild.reset();
        leftTuple = null;
        rightTuple = null;
    }

    /**
     * Evaluate the join condition between the left and right tuples. This method uses the ConditionEvaluator class.
     * @param leftTuple The left tuple.
     * @param rightTuple The right tuple.
     * @return True if the join condition is satisfied, false otherwise.
     */
    private boolean evaluateJoinCondition(Tuple leftTuple, Tuple rightTuple) {
        return evaluator.evaluateJoin(leftTuple, rightTuple);
    }
}
