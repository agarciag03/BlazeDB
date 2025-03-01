package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

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
            leftTuple = leftChild.getNextTuple();
        }

        while (leftTuple != null) {
            rightTuple = rightChild.getNextTuple();
            while (rightTuple != null) {
                Tuple joinedTuple = leftTuple.join(rightTuple);
                // cross product
                if (joinCondition == null || evaluateJoinCondition(joinedTuple)) {
                    return joinedTuple;
                }
                rightTuple = rightChild.getNextTuple();
            }
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

    private boolean evaluateJoinCondition(Tuple tuple) {
        return true;
//        ConditionEvaluator conditionEvaluator = new ConditionEvaluator(joinCondition);
//        return conditionEvaluator.evaluate(tuple);
    }
}
