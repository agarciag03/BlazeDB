package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;


import java.util.*;

public class SumOperator extends Operator {
    private Operator child;
    private List<Integer> groupByColumns;  // Índices de las columnas para GROUP BY
    private List<Function> sumExpressions;  // Expresiones SUM en la consulta

    private Map<List<Integer>, List<Tuple>> groupedTuples;  // Mapa para agrupar tuplas
    private Map<List<Integer>, List<Integer>> sumResults;  // Mapa de SUM por grupo
    private Iterator<List<Integer>> outputIterator;  // Iterador para los resultados


    public SumOperator(Operator child, List<Expression> groupByElements, List<Function> sumExpressions) throws Exception {
        this.child = child;
        this.groupByColumns = getColumnIndexes(groupByElements);
        this.sumExpressions = sumExpressions;

        this.groupedTuples = new HashMap<>();
        this.sumResults = new HashMap<>();
        processAggregation(); // Procesamos las tuplas en el constructor
        this.outputIterator = sumResults.keySet().iterator();
    }

    private List<Integer> getColumnIndexes(List<Expression> groupByElements) {
        List<Integer> groupByColumns = new ArrayList<>();
        for (Expression expression : groupByElements) {
            Column column = (Column) expression;
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();
            int columnIndex = Catalog.getInstance().getColumnIndex(tableName, columnName);
            groupByColumns.add(columnIndex);
        }
        return groupByColumns;
    }

    private void processAggregation() throws Exception{
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            List<Integer> groupKey = getGroupKey(tuple);
            groupedTuples.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(tuple);
        }
        // Ahora procesamos SUM para cada grupo
        for (List<Integer> groupKey : groupedTuples.keySet()) {
            List<Tuple> tuples = groupedTuples.get(groupKey);
            List<Integer> sumValues = computeSums(tuples);
            sumResults.put(groupKey, sumValues);
        }
    }

    private List<Integer> getGroupKey(Tuple tuple) {
        List<Integer> groupKey = new ArrayList<>();
        for (int columnIndex : groupByColumns) {
            groupKey.add((Integer) tuple.getValue(columnIndex));
        }
        return groupKey;
    }

    private List<Integer> computeSums(List<Tuple> tuples) {
        List<Integer> sums = new ArrayList<>(Collections.nCopies(sumExpressions.size(), 0));

        for (Tuple tuple : tuples) {
            for (int i = 0; i < sumExpressions.size(); i++) {
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
//                Expression param = null;
//                Expression param2 = parameters.get(0);
                //Expression param = parameters.getExpressions().get(0);

                if (param instanceof LongValue) {
                    return (int) ((LongValue) param).getValue(); // SUM(1)
                } else if (param instanceof Column) {
                    String tableName = ((Column) param).getTable().getName();
                    String columnName = ((Column) param).getColumnName();
                    int columnIndex = Catalog.getInstance().getColumnIndex(tableName, columnName);
                    return tuple.getValue(columnIndex); // SUM(A)
                } else if (param instanceof BinaryExpression) {
                    return evaluateBinaryExpression((BinaryExpression) param, tuple); // SUM(A * B)
                }
            }
        }
        return 0;
    }

    private int evaluateBinaryExpression(BinaryExpression expr, Tuple tuple) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();


        Catalog catalog = Catalog.getInstance();
        String tableName = ((Column) left).getTable().getName();
        String columnName = ((Column) left).getColumnName();
        int columnIndex = catalog.getColumnIndex(tableName, columnName);
        int leftValue = tuple.getValue(columnIndex);

        String tableName2 = ((Column) left).getTable().getName();
        String columnName2 = ((Column) left).getColumnName();
        int columnIndex2 = catalog.getColumnIndex(tableName2, columnName2);
        int rightValue = tuple.getValue(columnIndex2);

        if (expr instanceof Multiplication) {
            return leftValue * rightValue;
        }
        return 0;
    }

    @Override
    public Tuple getNextTuple() {
        if (outputIterator.hasNext()) {
            List<Integer> groupKey = outputIterator.next();
            List<Integer> sums = sumResults.get(groupKey);
            Tuple tuple = new Tuple();
            tuple.addValues(groupKey);
            tuple.addValues(sums);
            return tuple; //(groupKey, sums);
        }
        return null;
    }
//    public Tuple getNextTuple() throws Exception {
//        if (outputIterator == null) { // It is the first time it is called
//            Map<List<Integer>, List<Tuple>> groupTuples = groupTuples();
//            outputIterator = groupTuples.keySet().iterator();
//            if (sumExpressions != null) {
//                // Just in case we have sum
//            }
//        }
//
//        return null;
//    }

    @Override
    public void reset() throws Exception{
        child.reset();
        this.outputIterator = sumResults.keySet().iterator();
    }
}
