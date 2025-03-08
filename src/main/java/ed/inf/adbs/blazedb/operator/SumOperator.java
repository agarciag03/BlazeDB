package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.GroupByElement;

import java.util.*;

public class SumOperator extends Operator {
    private Operator child;
    private List<Expression> groupByElements;
    private List<Integer> groupByColumns;
    //private GroupByElement groupByColumns; // columns to group by
    private List<Expression> sumExpressions;
    private List<Tuple> aggregatedResults;
    private Iterator<Tuple> outputIterator; // Iterador para devolver resultados

    //private List<Function> sumColumn; // column to sum
    //private Map<Integer, Integer> groupedResults;// Mapping from group by columns to sum
    //private Iterator<Map.Entry<Integer, Integer>> iterator; // Iterador para recorrer los resultados //Internal Buffer // Iterador para recorrer los resultados //Internal Buffer
    //private boolean processed = false; // Flag to check if the results have been processed

    public SumOperator(Operator child, List<Expression> groupByElements, List<Expression> sumExpressions) {
        this.child = child;
        this.groupByElements = groupByElements;
        this.groupByColumns = getColumnIndexes(groupByElements);
        this.sumExpressions = sumExpressions;
        this.aggregatedResults = new ArrayList<>();
        this.outputIterator = null; // iterator tu return results
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


    @Override
    public Tuple getNextTuple() throws Exception {
        if (outputIterator == null) { // It is the first time it is called
            //processAggregation();
            Map<List<Integer>, List<Tuple>> groupTuples = groupTuples();
            for (Expression sumExpression: sumExpressions) {
                Function sumFunction = (Function) sumExpression;
                ExpressionList parameters = ((Function) sumExpression).getParameters();


            }
        }

        return null;
    }

            //for ( Expression sumExpression: sumExpressions) {
//                if (sumExpression instanceof Function) {
//                    Function function = (Function) sumExpression;
//                    String columnName = function.getParameters().getExpressions().get(0).toString();
//                    int columnIndex = Catalog.getInstance().getColumnIndex(columnName);
//                    Map<List<Integer>, Integer> aggregatedResults = aggregateSums(groupTuples, columnIndex);
//                    //outputIterator = aggregatedResults.iterator();
                //}
            //}
//            if (sumExpressions != null) {
//                System.out.println("SumOperator: sumExpressions is not null");
//                //if (sumExpressions instanceof longValue)
//                //Map<List<Integer>, Integer> aggregatedResults = aggregateSums(groupTuples, 0);
//                //outputIterator = aggregatedResults.iterator();
//            }
            //Map<List<Integer>, Integer> aggregatedResults = aggregateSums(groupTuples, 0);
            //outputIterator = aggregatedResults.iterator();
        //}
//        if (outputIterator.hasNext()) {
//            return outputIterator.next();
//        } else {
//            return null;
//        }



    @Override
    public void reset() throws Exception {
        child.reset();
        outputIterator = null;
    }

    private Map<List<Integer>, List<Tuple>> groupTuples() throws Exception {
        Map<List<Integer>, List<Tuple>> groupedTuples = new HashMap<>();
        Tuple tuple;
        //for (Tuple tuple : tuples) {
        while ((tuple = child.getNextTuple()) != null) {
            List<Integer> groupKey = new ArrayList<>();

            // Agrupar las tuplas por las columnas indicadas en groupByColumns
            for (int columnIndex : groupByColumns) {
                groupKey.add((Integer) tuple.getValue(columnIndex));
            }

            // Almacenar las tuplas agrupadas en el mapa
            groupedTuples.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(tuple);
        }

        return groupedTuples;
    }

    private Map<List<Integer>, Integer> aggregateSums(Map<List<Integer>, List<Tuple>> groupedTuples, int aggregationColumnIndex) {
        Map<List<Integer>, Integer> aggregatedResults = new HashMap<>();

        // Realizar la agregación (SUM) para cada grupo
        for (Map.Entry<List<Integer>, List<Tuple>> entry : groupedTuples.entrySet()) {
            List<Integer> groupKey = entry.getKey();
            List<Tuple> groupTuples = entry.getValue();

            int sum = 0;
            for (Tuple tuple : groupTuples) {
                sum += (Integer) tuple.getValue(aggregationColumnIndex); // Sumar el valor de la columna de agregación
            }

            // Almacenar el resultado de la agregación
            aggregatedResults.put(groupKey, sum);
        }

        return aggregatedResults;
    }


    private void processAggregation() throws Exception {
        // HashMap to store the aggregated results with the sum of integers for each group
        Map<List<Integer>, Integer> groupSums = new HashMap<>();
        Tuple tuple;

        while ((tuple = child.getNextTuple()) != null) {

            // Creating the grouping key from the values of the columns specified in groupByColumns
            List<Integer> groupKey = new ArrayList<>();
            for (int columnIndex : groupByColumns) {
                groupKey.add((Integer) tuple.getValue(columnIndex)); // Extrae el valor de cada columna y lo agrega a groupKey
            }

            // Si la clave ya existe, agrega el valor de la tupla a la suma. Si no, crea una nueva entrada en el mapa
            //groupSums.merge(groupKey, (Integer) tuple.getValue(aggregationColumn), Integer::sum);
        }

        // Ahora puedes procesar los resultados de la agregación en groupSums
//        for (Map.Entry<List<Integer>, Integer> entry : groupSums.entrySet()) {
//            List<Integer> key = entry.getKey();
//            Integer sum = entry.getValue();
//            System.out.println("Group: " + key + " -> SUM: " + sum);
//        }
    }
}
