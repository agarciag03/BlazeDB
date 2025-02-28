package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProjectOperator extends Operator {
    private Operator child;
    private List<Integer> columnIndexes;

    // Constructor
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
        return projectTuple(child.getNextTuple(), columnIndexes);
    }

    @Override
    public void reset() throws Exception {
        child.reset();
    }

    // Method to project a tuple based on the column indexes
    public Tuple projectTuple(Tuple tuple, List<Integer> columnIndexes) {
        //organise the code to be the same
        if (tuple != null) {
            Tuple projectedTuple = new Tuple();
            for (int index : columnIndexes) {
                projectedTuple.addValue(tuple.getValue(index));
            }
            return projectedTuple;
        }
        return null;
    }

}
