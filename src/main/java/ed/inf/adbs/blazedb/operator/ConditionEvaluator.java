package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
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
        condition.accept(this);
        return result;
    }



//    @Override
//    public void visit(EqualsTo equalsTo) {
//        equalsTo.getLeftExpression().accept(this);
//        int leftValue = Integer.parseInt(this.getBuffer().toString().trim());
//        this.getBuffer().setLength(0); // Clear the buffer
//
//        equalsTo.getRightExpression().accept(this);
//        int rightValue = Integer.parseInt(this.getBuffer().toString().trim());
//        this.getBuffer().setLength(0); // Clear the buffer
//
//        result = leftValue == rightValue;
//    }

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

    // Review it better binary expression
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

        int columnValue = tuple.getValue(columnIndex);
        this.getBuffer().append(columnValue);

    }

}
