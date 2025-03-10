package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator extends Operator {
    private Operator child;
    private List<Integer> columnIndexes;
    private List<String> columnItems;


    public ProjectOperator(Operator child, List<Expression> selectItems) {
        this.child = child;
        this.columnItems = new ArrayList<>();

        for (Expression expression : selectItems) {
            if (expression != null) {
                Column column = (Column) expression;
//                String tableName = column.getTable().getName();
//                String columnName = column.getColumnName();
//                String columnProjection = tableName + "." + columnName;
                //int columnIndex = Catalog.getInstance().getColumnIndex(tableName, columnName);
                columnItems.add(column.toString());
            }
        }
    }

//    public ProjectOperator(Operator child, List<Expression> selectItems) {
//        this.child = child;
//        this.columnIndexes = new ArrayList<>();
//
//        for (Expression expression : selectItems) {
//            if (expression != null) {
//                Column column = (Column) expression;
//                String tableName = column.getTable().getName();
//                String columnName = column.getColumnName();
//                int columnIndex = Catalog.getInstance().getColumnIndex(tableName, columnName);
//                columnIndexes.add(columnIndex);
//            }
//        }
//    }

    @Override
    public Tuple getNextTuple() throws Exception {
        return projectTuple(child.getNextTuple(), columnItems);
    }

//    public Tuple getNextTuple() throws Exception {
//        return projectTuple(child.getNextTuple(), columnIndexes);
//    }

    @Override
    public void reset() throws Exception {
        child.reset();
    }

    // Method to project a tuple based on the column indexes
    public Tuple projectTuple(Tuple tuple, List<String> columnItems) {
        //organise the code to be the same
        if (tuple != null) {
            Tuple projectedTuple = new Tuple();
            for (String column : columnItems) {
                int columnIndex = tuple.getColumnIndex(column);
                projectedTuple.addValue(tuple.getValue(columnIndex), column);
                //projectedTuple.addValue((tuple.getValue(columnIndex));)
                //projectedTuple.
            }
            return projectedTuple;
        }
        return null;
    }

    // Method to project a tuple based on the column indexes
//    public Tuple projectTuple2(Tuple tuple, List<Integer> columnIndexes) {
//        //organise the code to be the same
//        if (tuple != null) {
//            Tuple projectedTuple = new Tuple();
//            for (int index : columnIndexes) {
//                projectedTuple.addValue(tuple.getValue(index));
//            }
//            return projectedTuple;
//        }
//        return null;
//    }

}
