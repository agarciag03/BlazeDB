package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator extends Operator {
    private Operator child;
    private List<String> columnItems;


    public ProjectOperator(Operator child, List<Expression> selectItems) {
        this.child = child;
        this.columnItems = new ArrayList<>();

        for (Expression expression : selectItems) {
            if (expression != null) {
                Column column = (Column) expression;
                columnItems.add(column.toString());
            }
        }
    }

    @Override
    public Tuple getNextTuple() throws Exception {
        return projectTuple(child.getNextTuple(), columnItems);
    }

    @Override
    public void reset() throws Exception {
        child.reset();
    }

    // Method to project a tuple based on the column indexes
    public Tuple projectTuple(Tuple tuple, List<String> columnItems) {
        if (tuple != null) {
            Tuple projectedTuple = new Tuple();
            for (String column : columnItems) {
                int columnIndex = tuple.getColumnIndex(column);
                projectedTuple.addValue(tuple.getValue(columnIndex), column);
            }
            return projectedTuple;
        }
        return null;
    }
}
