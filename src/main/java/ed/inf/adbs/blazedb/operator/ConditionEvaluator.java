package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class ConditionEvaluator extends ExpressionDeParser {
    private Expression condition;
    private Tuple tuple;
    private boolean result;

    public ConditionEvaluator(Expression condition) {
        this.condition = condition;
    }

    public boolean evaluate(Tuple tuple) {
        this.tuple = tuple;
        return evaluateExpression(condition);
    }

    public boolean evaluateJoin(Tuple tuple, BinaryExpression binaryExpression, int rightValue) {
        this.tuple = tuple;
        return evaluateJoinExpression(binaryExpression, rightValue);
    }

    private boolean evaluateExpression(Expression expression) {
        if (expression instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) expression;
            return evaluateExpression(andExpr.getLeftExpression()) &&
                    evaluateExpression(andExpr.getRightExpression());
        } else {
            expression.accept(this);
            return result;
        }
    }

    // method for comparisons
    private void evaluateBinaryExpression(BinaryExpression binaryExpression) {
        int leftvalue = evaluateExpressionValue(binaryExpression.getLeftExpression()); // only integers
        int rightValue = evaluateExpressionValue(binaryExpression.getRightExpression());

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

    // This method is for join condition where we have to compare with right value from the right tuple
    public boolean evaluateJoinExpression(BinaryExpression binaryExpression, int rightValue) {
        int leftvalue = evaluateExpressionValue(binaryExpression.getLeftExpression()); // only integers

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
        return result;
    }

    // extract values from columns: two types columns or longValues
    private int evaluateExpressionValue(Expression expression) {
        if (expression instanceof Column) {
            Column column = (Column) expression;
            Catalog catalog = Catalog.getInstance();
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();
            int columnIndex = catalog.getColumnIndex(tableName, columnName);
            return tuple.getValue(columnIndex);
        } else {
            // if longValue
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
