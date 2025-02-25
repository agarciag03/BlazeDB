package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProjectOperator extends Operator {
    private Operator child;
    private List<Integer> columnIndexes;

    public ProjectOperator(Operator child, List<SelectItem> selectItems) {
        this.child = child;
        this.columnIndexes = new ArrayList<>();

        // Extract the column indexes from the select
        for (SelectItem item : selectItems) {
            Expression expression = item.getExpression();
            if (expression != null) {
                Column column = (Column) expression;
                String tableName = column.getTable().getName();
                String columnName = column.getColumnName();
                int columnIndex = Catalog.getInstance().getColumnIndex(tableName, columnName);
                columnIndexes.add(columnIndex);
            }
        }
    }

    @Override
    public Tuple getNextTuple() throws Exception {
        Tuple tuple = child.getNextTuple();
        if (tuple != null) {
            Tuple projectedTuple = new Tuple();
            for (int index : columnIndexes) {
                projectedTuple.addValue(tuple.getValue(index));
            }
            return projectedTuple;
        }
        return null;

    }

    @Override
    public void reset() throws Exception {
        child.reset();
    }
}
