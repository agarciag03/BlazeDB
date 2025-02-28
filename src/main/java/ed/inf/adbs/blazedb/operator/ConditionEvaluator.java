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
        return evaluateExpression(condition, tuple);
    }

    // Recurrent function to evaluate the expression with  many ANDs
    private boolean evaluateExpression(Expression expression, Tuple tuple) {
        this.tuple = tuple;
        if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            return evaluateExpression(andExpression.getLeftExpression(), tuple) &&
                    evaluateExpression(andExpression.getRightExpression(), tuple);
        } else {
            expression.accept(this);
            return result;
        }
    }

    /*
    public boolean evaluate(Tuple tuple) {
        boolean result = false;
        if (condition instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) condition;
            while (andExpression.getLeftExpression() != null && andExpression.getRightExpression() != null) {
                return result && evaluateAndExpression(andExpression, tuple);
            }
//            boolean evaluateLeftExpressionExpression = evaluateExpression(andExpression.getLeftExpression(), tuple);
//            boolean evaluateRightExpression = evaluateExpression(andExpression.getRightExpression(), tuple);
//            //return evaluateExpression(andExpression.getLeftExpression(), tuple) && evaluateExpression(andExpression.getRightExpression(), tuple);
//            return evaluateLeftExpressionExpression && evaluateRightExpression;
        }
        else {
            return evaluateExpression(condition, tuple);
        }
//        this.tuple = tuple;
//        condition.accept(this);
//        return result;
    }



    private boolean evaluateAndExpression(AndExpression andExpression, Tuple tuple) {
        boolean evaluateLeftExpressionExpression = evaluateExpression(andExpression.getLeftExpression(), tuple);
        boolean evaluateRightExpression = evaluateExpression(andExpression.getRightExpression(), tuple);
        return evaluateLeftExpressionExpression && evaluateRightExpression;
    }

    private boolean evaluateExpression(Expression expression, Tuple tuple) {
        this.tuple = tuple;
        expression.accept(this);
        return result;
//        if (expression instanceof EqualsTo){
//
//        } else if (expression instanceof NotEqualsTo){
//
//        } else if (expression instanceof GreaterThan){
//
//        } else if (expression instanceof GreaterThanEquals){
//
//        } else if (expression instanceof MinorThan){
//
//        } else if (expression instanceof MinorThanEquals){
//
//        } else if (expression instanceof Column){
//
//        }
//        return false;
    }
*/

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
