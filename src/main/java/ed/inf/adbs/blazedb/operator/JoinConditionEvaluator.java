package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class JoinConditionEvaluator extends ExpressionDeParser {
    private Expression condition;
    private Tuple leftTuple;
    private Tuple rightTuple;
    private boolean result;

    public JoinConditionEvaluator(Expression condition) {
        this.condition = condition;
    }

    public boolean evaluate(Tuple leftTuple, Tuple rightTuple) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
        return evaluateExpression(condition);
    }

    private boolean evaluateExpression(Expression expression) {
        if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            return evaluateExpression(andExpression.getLeftExpression()) &&
                    evaluateExpression(andExpression.getRightExpression());
        } else {
            expression.accept(this);
            return result;
        }
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        evaluateBinaryExpression(equalsTo, "==");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        evaluateBinaryExpression(notEqualsTo, "!=");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        evaluateBinaryExpression(greaterThan, ">");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        evaluateBinaryExpression(greaterThanEquals, ">=");
    }

    @Override
    public void visit(MinorThan minorThan) {
        evaluateBinaryExpression(minorThan, "<");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        evaluateBinaryExpression(minorThanEquals, "<=");
    }

    private void evaluateBinaryExpression(BinaryExpression binaryExpression, String comparison) {
        binaryExpression.getLeftExpression().accept(this);
        int leftValue = Integer.parseInt(this.getBuffer().toString().trim());
        this.getBuffer().setLength(0); // Clear the buffer

        binaryExpression.getRightExpression().accept(this);
        int rightValue = Integer.parseInt(this.getBuffer().toString().trim());
        this.getBuffer().setLength(0); // Clear the buffer

        switch (comparison) {
            case "==":
                result = leftValue == rightValue;
                break;
            case "!=":
                result = leftValue != rightValue;
                break;
            case ">":
                result = leftValue > rightValue;
                break;
            case ">=":
                result = leftValue >= rightValue;
                break;
            case "<":
                result = leftValue < rightValue;
                break;
            case "<=":
                result = leftValue <= rightValue;
                break;
        }
    }

    @Override
    public void visit(Column column) {
        Catalog catalog = Catalog.getInstance(); // because of singleton pattern

        String columnName = column.getColumnName();
        String tableName = column.getTable().getName();
        int columnIndex = catalog.getColumnIndex(tableName, columnName);

        int columnValue = leftTuple.getValue(columnIndex); // Assuming comparison is between columns of the same index
        this.getBuffer().append(columnValue);
    }
}
