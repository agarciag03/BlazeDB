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

    @Override
    public Tuple getNextTuple() throws Exception {

        // blocking operator
        if (outputIterator == null) {
            this.groupedTuples = new HashMap<>();
            this.aggregateResults = new HashMap<>();
            processGroupByAndSum();
            this.outputIterator = aggregateResults.keySet().iterator();
        }

        if (outputIterator.hasNext()) {
            List<Integer> groupKey = outputIterator.next();
            List<Integer> sums = aggregateResults.get(groupKey);
            Tuple tuple = new Tuple();

            // The SUM functions, if present, can reference any columns, including non-group-by columns. so we could have either group-by columns or sum columns or both
            // therefore, here, the result returned will depend on the presence of group-by columns and sum columns

            // return tuples depending on GroupBy and Sum
            // The SELECT list can only list group-by columns but not necessarily all of them, so here we check if the projectionColumns are empty, if no, there are some columns to project
            // some group-by columns can be omitted from the output
            if (!groupByColumnsNames.isEmpty() && !projectionColumns.isEmpty()) {
                List<Integer> filteredGroupKey = filterGroupKey(groupKey, groupByColumnsNames, projectionColumns);
                tuple.addValues(filteredGroupKey, projectionColumns);
                //tuple.addValues(groupKey, groupByColumnsNames);

            }


            if (!sumExpressions.isEmpty()) {
                tuple.addValues(sums, getSumExpressions(this.sumExpressions));
            }
            tuple.printTupleWithColumns();
            return tuple;
        }
        return null;
    }

    @Override
    public void reset() throws Exception{
        child.reset();
        this.outputIterator = aggregateResults.keySet().iterator();
    }

    private List<String> getColumns(List<Expression> expressionElements) {
        List<String> columnNames = new ArrayList<>();
        for (Expression expression : expressionElements) {
            Column column = (Column) expression;
            columnNames.add(column.toString());
        }
        return columnNames;
    }

    private void processGroupByAndSum() throws Exception {
        Tuple tuple;

        // processing Groupby based on the columns - organizes tuples into
        //groups
        while ((tuple = child.getNextTuple()) != null) {
            List<Integer> groupKey = getGroupKey(tuple);
            groupedTuples.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(tuple);
        }

        for (List<Integer> groupKey : groupedTuples.keySet()) {
            // for each group computes aggregate values
            // processing Sum based on the group by, using the whole tuple
            if (!sumExpressions.isEmpty()) {
                List<Tuple> tuples = groupedTuples.get(groupKey);
                List<Integer> sumValues = computeSums(tuples);
                aggregateResults.put(groupKey, sumValues);
            } else {
                aggregateResults.put(groupKey,null);
            }
        }

    }

    private List<Integer> filterGroupKey(List<Integer> groupKey, List<String> groupByColumnsNames, List<String> projectionColumns) {
        List<Integer> filteredGroupKey = new ArrayList<>();

        for (String projectionColumn : projectionColumns) {
            int index = groupByColumnsNames.indexOf(projectionColumn);
            if (index != -1) { // Si la columna proyectada está en groupByColumnsNames
                filteredGroupKey.add(groupKey.get(index));
            }
        }

        return filteredGroupKey;
    }

    private List<Integer> getGroupKey(Tuple tuple) {
        List<Integer> groupKey = new ArrayList<>();
        if(groupByColumnsNames.isEmpty()) {
            // if there is not Groupby columns, we group by all columns -1
            groupKey.add(-1);
            return groupKey;
        } else {
            for (String column : groupByColumnsNames) {
                groupKey.add((Integer) tuple.getValue(column));
            }
        }
        return groupKey;
    }

    private List<Integer> computeSums(List<Tuple> tuples) {
        List<Integer> sums = new ArrayList<>(Collections.nCopies(sumExpressions.size(), 0));
        for (Tuple tuple : tuples) {
            for (int i = 0; i < sumExpressions.size(); i++) {
                // evaluate the sum expression: constant, column or multiplication
                sums.set(i, sums.get(i) + evaluateSum(sumExpressions.get(i), tuple));
            }
        }
        return sums;
    }

    // Instructions: Each SUM can take as argument one term (integer or column) or a product of terms.(BinaryExpression)
    private int evaluateSum(Expression expression, Tuple tuple) {
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
                    return multiplicacionExpression((BinaryExpression) param, tuple); // SUM(A * B)
                }
            }
        }
        return 0;
    }

    private int multiplicacionExpression(Expression expr, Tuple tuple) {
        if (expr instanceof Column) {
            // Si la expresión es una columna, obtenemos su valor del tuple
            Column column = (Column) expr;
            return tuple.getValue(column.toString());
        } else if (expr instanceof LongValue) {
            // Si la expresión es un valor numérico (ej. 5, 10, etc.)
            return (int) ((LongValue) expr).getValue();
        } else if (expr instanceof Multiplication) {
            // Si es una multiplicación, descomponemos en izquierda y derecha recursivamente
            BinaryExpression multiplication = (BinaryExpression) expr;
            int leftValue = multiplicacionExpression(multiplication.getLeftExpression(), tuple);
            int rightValue = multiplicacionExpression(multiplication.getRightExpression(), tuple);
            return leftValue * rightValue;
        } else {
            throw new IllegalArgumentException("Tipo de expresión no soportado: " + expr);
        }
    }

    private  List <String> getSumExpressions(List<Function> sumExpressions) {
        List<String> sumExpressionsList = new ArrayList<>();
        for (Function function : sumExpressions) {
            sumExpressionsList.add(function.toString());
        }
        return sumExpressionsList;
    }

}
