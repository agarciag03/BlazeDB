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

public class SumOperator extends Operator {
    private Operator child;
    private List<String> groupByColumnsNames;
    private List<Function> sumExpressions;

    private Map<List<Integer>, List<Tuple>> groupedTuples;
    private Map<List<Integer>, List<Integer>> aggregateResults;
    private Iterator<List<Integer>> outputIterator;

    private boolean projection;  // Proyección de columnas

    public SumOperator(Operator child, List<Expression> groupByElements, List<Function> sumExpressions, Boolean projection) {
        this.child = child;
        this.groupByColumnsNames = getColumnGroupBy(groupByElements);
        this.sumExpressions = sumExpressions;
        this.projection = projection;
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

            // return tuples depending on GroupBy and Sum
            if (!groupByColumnsNames.isEmpty() && projection) {
                tuple.addValues(groupKey, groupByColumnsNames);
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

    private List<String> getColumnGroupBy(List<Expression> groupByElements) {
        this.groupByColumnsNames = new ArrayList<>();
        for (Expression expression : groupByElements) {
            Column column = (Column) expression;
            groupByColumnsNames.add(column.toString());
        }
        return groupByColumnsNames;
    }

    private void processGroupByAndSum() throws Exception {
        Tuple tuple;

        // processing Groupby
        while ((tuple = child.getNextTuple()) != null) {
            List<Integer> groupKey = getGroupKey(tuple);
            groupedTuples.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(tuple);
        }

        for (List<Integer> groupKey : groupedTuples.keySet()) {

            // processing Sum
            if (!sumExpressions.isEmpty()) {
                List<Tuple> tuples = groupedTuples.get(groupKey);
                List<Integer> sumValues = computeSums(tuples);
                aggregateResults.put(groupKey, sumValues);
            } else {
                aggregateResults.put(groupKey,null);
            }
        }

    }

    private List<Integer> getGroupKey(Tuple tuple) {
        List<Integer> groupKey = new ArrayList<>();
        if(groupByColumnsNames.isEmpty()) {
            // if there is not Groupby columns, we group by all columns -1
            groupKey.add(-1);
            return groupKey;
        } else {
            for (String column : groupByColumnsNames) {
                groupKey.add((Integer) tuple.getValue(tuple.getColumnIndex(column)));
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

    private int evaluateSum(Expression expression, Tuple tuple) {
        if (expression instanceof Function) {
            ExpressionList parameters = ((Function) expression).getParameters();

            if (parameters != null && parameters.getExpressions().size() == 1) {
                Expression param = (Expression) parameters.getExpressions().get(0);

                if (param instanceof LongValue) { // Sum by constant
                    return (int) ((LongValue) param).getValue();

                } else if (param instanceof Column) { // Sum by column
                    Column column = (Column) param;
                    return tuple.getValue(tuple.getColumnIndex(column.toString()));

                } else if (param instanceof BinaryExpression) { // sum multiplication
                    return multiplicacionExpression((BinaryExpression) param, tuple); // SUM(A * B)
                }
            }
        }
        return 0;
    }

//    private int multiplicacionExpression(BinaryExpression expr, Tuple tuple) {
//
//        Expression leftExpression = expr.getLeftExpression();
//        Column leftColumn = (Column) leftExpression;
//        int leftValue = tuple.getValue(tuple.getColumnIndex(leftColumn.toString()));
//
//        Expression rightExpression = expr.getRightExpression();
//        Column rightColumn = (Column) rightExpression;
//        int rightValue = tuple.getValue(tuple.getColumnIndex(rightColumn.toString()));
//
//        if (expr instanceof Multiplication) {
//            return leftValue * rightValue;
//        }
//        return 0;
//    }

    private int multiplicacionExpression(Expression expr, Tuple tuple) {
        if (expr instanceof Column) {
            // Si la expresión es una columna, obtenemos su valor del tuple
            Column column = (Column) expr;
            return tuple.getValue(tuple.getColumnIndex(column.toString()));
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
