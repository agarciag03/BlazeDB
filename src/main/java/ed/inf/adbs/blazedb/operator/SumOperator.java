package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.GroupByElement;

import java.util.*;

public class SumOperator extends Operator {
    private Operator child;
    private GroupByElement groupByColumns; // columns to group by
    private List<Function> sumColumn; // column to sum
    private Map<Integer, Integer> groupedResults;// Mapping from group by columns to sum
    private Iterator<Map.Entry<Integer, Integer>> iterator; // Iterador para recorrer los resultados //Internal Buffer // Iterador para recorrer los resultados //Internal Buffer
    private boolean processed = false; // Flag to check if the results have been processed

    public SumOperator(Operator child, GroupByElement groupByColumns, List<Function> sumColumn) {
        this.child = child;
        this.groupByColumns = groupByColumns;
        this.sumColumn = sumColumn;
        this.groupedResults = new HashMap<>(); // Hash table to store the results
        this.iterator = null;
    }

    private void processTuples() throws Exception {
        groupedResults.clear();
        Tuple tuple;

        while ((tuple = child.getNextTuple()) != null) {
            List<Object> groupKey = new ArrayList<>();
//            for (String col : groupByColumns) {
//                groupKey.add(tuple.getValue(col)); // Extraemos valores para agrupar
//            }

//            int sumValue = (int) tuple.getValue(sumColumn); // Extraemos el valor de SUM
//            groupedResults.put(groupKey, groupedResults.getOrDefault(groupKey, 0) + sumValue);
        }

        iterator = groupedResults.entrySet().iterator(); // Preparamos el iterador
        processed = true;
    }

    @Override
    public Tuple getNextTuple() throws Exception {
        if (!processed) {
            processTuples(); // Procesamos solo la primera vez
        }
        if (iterator.hasNext()) {
            Map.Entry<Integer, Integer> entry = iterator.next();
            Integer key = entry.getKey(); // ID del producto
            Integer value = entry.getValue(); // Suma de precios
            Tuple tuple = new Tuple();
            tuple.addValue(key);
            tuple.addValue(value);
            return tuple;
        }
        return null; // No hay más resultados
    }

    @Override
    public void reset() throws Exception {
        child.reset();
        groupedResults.clear();
        iterator = null;
        processed = false;
    }
}
