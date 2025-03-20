package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class ConditionEvaluator extends ExpressionDeParser {
    private Expression condition;
    private Tuple tuple;
    private Tuple leftTuple;
    private Tuple rightTuple;
    private boolean result;

    public ConditionEvaluator(Expression condition) {
        this.condition = condition;
    }

    public boolean evaluate(Tuple tuple) {
        this.tuple = tuple;
        return evaluateExpression(condition);
    }

    public boolean evaluateJoin(Tuple lefttuple, Tuple rightTuple) {
        this.leftTuple = lefttuple;
        this.rightTuple = rightTuple;
        return evaluateExpression(condition);
    }

    private boolean evaluateExpression(Expression expression) {
        expression.accept(this);
        return result;
    }

    private void evaluateBinaryExpression(BinaryExpression binaryExpression) {
        int leftvalue;
        int rightValue;

        if (this.tuple != null) { // if it is a projection
            leftvalue = evaluateExpressionValue(binaryExpression.getLeftExpression(), this.tuple);
            rightValue = evaluateExpressionValue(binaryExpression.getRightExpression(), this.tuple); // changed to
        } else { // if it is a join it means that we have two tuples
            leftvalue = evaluateExpressionValue(binaryExpression.getLeftExpression(), this.leftTuple); // only integers
            rightValue = evaluateExpressionValue(binaryExpression.getRightExpression(), this.rightTuple);
        }

        if (binaryExpression instanceof EqualsTo) {
            result = leftvalue == rightValue;
        } else if (binaryExpression instanceof NotEqualsTo) {
            result = leftvalue != rightValue;
        } else if (binaryExpression instanceof GreaterThan) {
            result = leftvalue > rightValue;
        } else if (binaryExpression instanceof GreaterThanEquals) {
            result = leftvalue >= rightValue;
        } else if (binaryExpression instanceof MinorThan) {
            result = leftvalue < rightValue;
        } else if (binaryExpression instanceof MinorThanEquals) {
            result = leftvalue <= rightValue;
        }
    }

    // extract values from columns: two types columns or longValues
    private int evaluateExpressionValue(Expression expression, Tuple tuple) {
        if (expression instanceof Column) {
            Column column = (Column) expression;
            int columnIndex = tuple.getColumnIndex(column.toString());
            return tuple.getValue(columnIndex);
        } else {
            // When the WHERE condition contains an integer instead of a column.
            return Integer.parseInt(expression.toString());
        }
    }

    // Methods visit for each type of expression
    @Override
    public void visit(EqualsTo equalsTo) {
        evaluateBinaryExpression(equalsTo);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        evaluateBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        evaluateBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        evaluateBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        evaluateBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        evaluateBinaryExpression(minorThanEquals);
    }
}
