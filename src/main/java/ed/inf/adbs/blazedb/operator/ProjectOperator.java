package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import java.util.List;

/**
 * The ProjectOperator class for the iterator model.
 * This class is responsible for projecting the columns of a tuple based on the column names provided.
 */
public class ProjectOperator extends Operator {
    private Operator child;
    private List<Expression> projectionExpressions;

    /**
     * Constructor for the ProjectOperator class
     * @param child The child operator
     * @param projectionExpressions The list of column names to be projected
     */
    public ProjectOperator(Operator child, List<Expression> projectionExpressions) {
        this.child = child;
        this.projectionExpressions = projectionExpressions;
    }

    /**
     * Retrieves the next tuple from the iterator and projects the columns based on the column names provided.
     * @return A Tuple with the projected columns
     */
    @Override
    public Tuple getNextTuple() throws Exception {
        return projectTuple(child.getNextTuple());
    }

    /**
     * Resets the iterator to the start.
     */
    @Override
    public void reset() throws Exception {
        child.reset();
    }

    /**
     * Projects the columns of a tuple based on the column names provided.
     * In order to project the columns, a new tuple is created with the projected columns.
     * @param tuple The original tuple from the child operator
     * @return A Tuple with the projected columns
     */
    public Tuple projectTuple(Tuple tuple) {
        if (tuple != null) {
            // Create a new tuple with the projected columns
            Tuple projectedTuple = new Tuple();
            for (Expression columnExpression : projectionExpressions) {
                Column column = (Column) columnExpression;
                projectedTuple.addValue(tuple.getValue(column.toString()), column.toString());
            }
            return projectedTuple;
        }
        return null;
    }
}
