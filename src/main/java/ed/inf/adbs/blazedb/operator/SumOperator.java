package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * SumOperator is responsible for computing the SUM aggregation, either with or without a GROUP BY clause.
 * This operator processes input tuples from a child operator, groups them based on specified
 *  * GROUP BY columns (if present), and computes the sum of the specified expressions for each group.
 *  * If no GROUP BY clause is specified, all tuples are treated as a single group. In this case, there will be just a group with label -1
 * The algorithm applies the following steps:
 *  * Scans all tuples from the child operator. It is a blocking operator.
 *  * Groups tuples based on the GROUP BY columns using a HashMap.
 *  * Computes SUM aggregations for each group.
 *  * Stores the results and returns tuples containing the computed sums.
 */
public class SumOperator extends Operator {
    private Operator child;
    private List<String> groupByColumnsNames;
    private List<Function> sumExpressions;
    private List<String> projectionColumns;

    private Map<List<Integer>, List<Tuple>> groupedTuples;
    private Map<List<Integer>, List<Integer>> aggregateResults;
    private Iterator<List<Integer>> outputIterator;

    /**
     * Constructor for the SumOperator class.
     * @param child The child operator.
     * @param groupByElements The list of group by elements.
     * @param sumExpressions The list of sum expressions.
     * @param projectionElements The list of projection elements.
     */
    public SumOperator(Operator child, List<Expression> groupByElements, List<Function> sumExpressions, List<Expression> projectionElements) {
        this.child = child;
        this.groupByColumnsNames = getColumns(groupByElements);
        this.sumExpressions = sumExpressions;
        this.projectionColumns = getColumns(projectionElements);
    }

    /**
     * This method transforms the list of expressions into a list of column names (String) for easier manipulation.
     * Both group by and projection elements are transformed into column names.
     * @throws Exception
     */
    private List<String> getColumns(List<Expression> expressionElements) {
        List<String> columnNames = new ArrayList<>();
        for (Expression expression : expressionElements) {
            Column column = (Column) expression;
            columnNames.add(column.toString());
        }
        return columnNames;
    }

    /**
     * This method returns the next tuple containing the computed sums and/or the group by columns.
     * @return The next tuple containing the computed sums/groupby columns.
     * @throws Exception
     */
    @Override
    public Tuple getNextTuple() throws Exception {

        // blocking operator, if the outputIterator is null all tuples should be processed
        if (outputIterator == null) {
            this.groupedTuples = new HashMap<>();
            this.aggregateResults = new HashMap<>();
            processGroupByAndSum();
            this.outputIterator = aggregateResults.keySet().iterator();
        }

        // if tuples were processed, return the results
        if (outputIterator.hasNext()) {
            List<Integer> groupKey = outputIterator.next();
            List<Integer> sums = aggregateResults.get(groupKey);
            Tuple tuple = new Tuple();

            // Since the group-by columns are not necessarily all projected,
            // It is needed to filter the groupKey to only include the columns that are projected
            if (!groupByColumnsNames.isEmpty() && !projectionColumns.isEmpty()) {
                List<Integer> filteredGroupKey = filterGroupKey(groupKey, groupByColumnsNames, projectionColumns);
                tuple.addValues(filteredGroupKey, projectionColumns);
            }

            // If there are sum expressions, add the sum values to the tuple.
            // The SUM expressions are always at the end of the tuple
            if (!sumExpressions.isEmpty()) {
                tuple.addValues(sums, getSumExpressions(sumExpressions));
            }
            return tuple;
        }
        return null;
    }

    /**
     * This method resets the operator setting the outputIterator again.
     * @throws Exception
     */
    @Override
    public void reset() throws Exception{
        child.reset();
        this.outputIterator = aggregateResults.keySet().iterator();
    }

    /**
     * This method processes all tuples from the child operator, grouping the whole tuple by the group by columns
     * and then, if it is needed, computing the sum of the specified expressions for each group.
     * @throws Exception
     */
    private void processGroupByAndSum() throws Exception {
        Tuple tuple;

        // processing all tuples from the child operator grouping them by the group by columns
        while ((tuple = child.getNextTuple()) != null) {
            List<Integer> groupKey = getGroupKey(tuple);
            groupedTuples.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(tuple);
        }

        // for each group computes aggregate values, selecting the sum columns needed for the sum
        for (List<Integer> groupKey : groupedTuples.keySet()) {
            if (!sumExpressions.isEmpty()) {
                List<Tuple> tuples = groupedTuples.get(groupKey);
                List<Integer> sumValues = computeSums(tuples);
                aggregateResults.put(groupKey, sumValues);
            } else {
                aggregateResults.put(groupKey,null);
            }
        }
    }

    /**
     * This method returns the group key for the tuple.
     * If there is no group by columns, the group key will be -1.
     * @param tuple The tuple to get the group key.
     * @return The group key for the tuple.
     */
    private List<Integer> getGroupKey(Tuple tuple) {
        List<Integer> groupKey = new ArrayList<>();
        if(groupByColumnsNames.isEmpty()) {
            // if there is not Groupby columns, there will be one group with all columns, labeled as -1
            groupKey.add(-1);
            return groupKey;
        } else {
            for (String column : groupByColumnsNames) {
                groupKey.add((Integer) tuple.getValue(column));
            }
        }
        return groupKey;
    }

    /**
     * This method filters the groupKey to only include the columns that are projected.
     * @param groupKey The group key to be filtered.
     * @param groupByColumnsNames The list of group by columns.
     * @param projectionColumns The list of projection columns.
     * @return The filtered group key.
     */
    private List<Integer> filterGroupKey(List<Integer> groupKey, List<String> groupByColumnsNames, List<String> projectionColumns) {
        List<Integer> filteredGroupKey = new ArrayList<>();

        for (String projectionColumn : projectionColumns) {
            int index = groupByColumnsNames.indexOf(projectionColumn);
            // if the column is not in the groupByColumnsNames, the index will be -1
            if (index != -1) {
                filteredGroupKey.add(groupKey.get(index));
            }
        }
        return filteredGroupKey;
    }

    /**
     * This method computes the sum of the specified expressions for each group. Three types of expressions
     * are supported: constant, column or multiplication.
     * @param tuples The list of tuples to compute the sum.
     * @return The list of sums.
     */
    private List<Integer> computeSums(List<Tuple> tuples) {
        List<Integer> sums = new ArrayList<>(Collections.nCopies(sumExpressions.size(), 0));
        for (Tuple tuple : tuples) {
            for (int i = 0; i < sumExpressions.size(); i++) {
                // evaluate the sum expression: constant, column or multiplication
                sums.set(i, sums.get(i) + obtainSum(sumExpressions.get(i), tuple));
            }
        }
        return sums;
    }

    /**
     * This method evaluates the sum expression. Three types of expressions are supported: constant, column or multiplication.
     * @param expression The expression to be evaluated.
     * @param tuple The tuple to get the value of the column.
     * @return The value of the sum expression.
     */
    private int obtainSum(Expression expression, Tuple tuple) {
        if (expression instanceof Function) {
            ExpressionList parameters = ((Function) expression).getParameters();

            if (parameters != null && parameters.getExpressions().size() == 1) {
                Expression param = (Expression) parameters.getExpressions().get(0);

                if (param instanceof LongValue) { // Sum by constant
                    return (int) ((LongValue) param).getValue();

                } else if (param instanceof Column) { // Sum by column
                    Column column = (Column) param;
                    return tuple.getValue(column.toString());

                } else if (param instanceof BinaryExpression) { // sum multiplication
                    // If the expression is a multiplication, the expression will be evaluated recursively
                    return obtainMultiplication((BinaryExpression) param, tuple); // SUM(A * B)
                }
            }
        }
        return 0;
    }

    private int obtainMultiplication(Expression expression, Tuple tuple) {
        if (expression instanceof Column) { // if the expression is a column
            Column column = (Column) expression;
            return tuple.getValue(column.toString());

        } else if (expression instanceof LongValue) { // if the expression is a constant
            return (int) ((LongValue) expression).getValue();

        } else if (expression instanceof Multiplication) { // if the expression is a multiplication
            // If the expression is a multiplication, the expression will be evaluated recursively
            BinaryExpression multiplication = (BinaryExpression) expression;
            int leftValue = obtainMultiplication(multiplication.getLeftExpression(), tuple);
            int rightValue = obtainMultiplication(multiplication.getRightExpression(), tuple);
            return leftValue * rightValue;
        } else {
            throw new IllegalArgumentException("It is not supported Expression " + expression);
        }
    }

    /**
     * This method returns the sum expressions as a list of strings to be added to the tuple as column names.
     * @param sumExpressions The list of sum expressions.
     * @return The list of sum expressions as strings.
     */
    private  List <String> getSumExpressions(List<Function> sumExpressions) {
        List<String> sumExpressionsList = new ArrayList<>();
        for (Function function : sumExpressions) {
            sumExpressionsList.add(function.toString());
        }
        return sumExpressionsList;
    }

}
