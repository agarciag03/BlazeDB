package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

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
//            if (validConditionTuple(tuple)) {
//                if (evaluator.evaluate(tuple)) {// Evaluate the given condition
////                return tuple;
//                }
//            }
        }
        return null;
    }

    @Override
    public void reset() throws Exception {
        child.reset();
    }

    private boolean validConditionTuple(Tuple tuple) {
        BinaryExpression binaryExpression = (BinaryExpression) condition;
        if (binaryExpression.getLeftExpression() instanceof Column) {
            Column column = (Column) binaryExpression.getLeftExpression();
            String columnName = column.getTable().getName();
            if (tuple.getColumnNames().contains(columnName)) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }
}

