package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * This class is responsible for evaluating the WHERE condition of a query (Selections and Joins conditions).
 * It uses the ExpressionDeParser class from the JSQLParser library to parse the WHERE condition.
 * The evaluate method is used to evaluate the WHERE condition for selections using a single tuple.
 * The evaluateJoin method is used to evaluate the WHERE condition for joins using two tuples.
 */
public class ConditionEvaluator extends ExpressionDeParser {
    // The WHERE condition of the query
    private Expression condition;
    // Tuple is used for selections and leftTuple and rightTuple are used for joins
    private Tuple tuple;
    private Tuple leftTuple;
    private Tuple rightTuple;
    // The result of the condition: true or false
    private boolean result;

    /**
     * Constructor for the ConditionEvaluator class.
     * @param condition The WHERE condition of the query.
     */
    public ConditionEvaluator(Expression condition) {
        this.condition = condition;
    }

    /**
     * Evaluate the WHERE condition for selections using a single tuple.
     * @param tuple The tuple to evaluate the condition.
     * @return The result of the condition: true or false.
     */
    public boolean evaluate(Tuple tuple) {
        this.tuple = tuple;
        return evaluateExpression(condition);
    }

    /**
     * Evaluate the WHERE condition for joins using two tuples.
     * @param lefttuple The left tuple to evaluate the condition.
     * @param rightTuple The right tuple to evaluate the condition.
     * @return The result of the condition: true or false.
     */
    public boolean evaluateJoin(Tuple lefttuple, Tuple rightTuple) {
        this.leftTuple = lefttuple;
        this.rightTuple = rightTuple;
        return evaluateExpression(condition);
    }

    /**
     * Expression Visitor to evaluate the WHERE condition.
     * @param expression The expression to evaluate.
     * @return The result of the expression: true or false.
     */
    private boolean evaluateExpression(Expression expression) {
        expression.accept(this);
        return result;
    }

    /**
     * Since all conditions are binaryExpression like S.A = E.B  or S.a = 1, this method is used
     * to evaluate the binary expression selecting the left and right values and comparing them.
     * @param binaryExpression The binary expression to evaluate, it corresponds to the WHERE condition.
     */
    private void evaluateBinaryExpression(BinaryExpression binaryExpression) {
        int leftvalue;
        int rightValue;

        if (this.tuple != null) {
            // if it is a selection it means just one tuple is used
            leftvalue = getExpressionValue(binaryExpression.getLeftExpression(), this.tuple);
            rightValue = getExpressionValue(binaryExpression.getRightExpression(), this.tuple);
        } else {
            // if it is a join it means that two tuples must be used
            leftvalue = getExpressionValue(binaryExpression.getLeftExpression(), this.leftTuple);
            rightValue = getExpressionValue(binaryExpression.getRightExpression(), this.rightTuple);
        }

        // Compare the left and right values based on the type of binary expression
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

    /**
     * Get the value of the expression from the tuple. There are two cases: column or integer.
     * @param expression This is the expression to extract the value.
     * @param tuple The tuple to extract the value.
     * @return The value of this column in the tuple or the integer value of the expression.
     */
    private int getExpressionValue(Expression expression, Tuple tuple) {

        if (expression instanceof Column) {
            // When the WHERE condition contains a column.
            Column column = (Column) expression;
            return tuple.getValue(column.toString());
        } else {
            // When the WHERE condition contains an integer.
            return Integer.parseInt(expression.toString());
        }
    }

    /**
     * Visit the EqualsTo expression.
     * @param equalsTo The EqualsTo expression to visit.
     */
    @Override
    public void visit(EqualsTo equalsTo) {
        evaluateBinaryExpression(equalsTo);
    }

    /**
     * Visit the NotEqualsTo expression.
     * @param notEqualsTo The NotEqualsTo expression to visit.
     */
    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        evaluateBinaryExpression(notEqualsTo);
    }

    /**
     * Visit the GreaterThan expression.
     * @param greaterThan The GreaterThan expression to visit.
     */
    @Override
    public void visit(GreaterThan greaterThan) {
        evaluateBinaryExpression(greaterThan);
    }

    /**
     * Visit the GreaterThanEquals expression.
     * @param greaterThanEquals The GreaterThanEquals expression to visit.
     */
    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        evaluateBinaryExpression(greaterThanEquals);
    }

    /**
     * Visit the MinorThan expression.
     * @param minorThan The MinorThan expression to visit.
     */
    @Override
    public void visit(MinorThan minorThan) {
        evaluateBinaryExpression(minorThan);
    }

    /**
     * Visit the MinorThanEquals expression.
     * @param minorThanEquals The MinorThanEquals expression to visit.
     */
    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        evaluateBinaryExpression(minorThanEquals);
    }
}
