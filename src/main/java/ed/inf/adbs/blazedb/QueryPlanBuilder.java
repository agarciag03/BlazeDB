package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.Operator;
import ed.inf.adbs.blazedb.operator.ProjectOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

public class QueryPlanBuilder {

    public static Operator buildQueryPlan(Statement statement) throws Exception {

        // Create a select
        Select select = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        boolean projection = false;
        boolean selection = false;
        boolean join = false;

        Operator rootOperator = null;

        //Identifying elements of the query

        String tableName = plainSelect.getFromItem().toString();
        Expression conditionExpression = plainSelect.getWhere();
        List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();
        List<?> joins = plainSelect.getJoins();
        //List<SelectItem<?>> selectItems = plainSelect.getSelectItems();

        // Building the tree

        // Review the tree of operators
        if (selectItems.size() >= 1){
            if (selectItems.get(0).getExpression() instanceof AllColumns) {
                projection = false;
            } else {
                projection = true;
            }
        }

        if (conditionExpression != null) {
            // Add binarytree to identify the elements of the conditions are tables and are differents to identify a joincondition
            selection = true;
        }
        if (joins != null) {
            join = true;
        }

        // Organising the tree of operators
        Operator scanOperator = new ScanOperator(tableName);
        rootOperator = scanOperator;

        if (selection) {
            // review later that I can fix this operators I need to find a way to optimise them
            Operator selectOperator = new SelectOperator(rootOperator, conditionExpression);
            rootOperator = selectOperator;
        }
        if (join) {
            // Add binarytree to identify the elements of the conditions are tables and are differents to identify a joincondition
        }

        // Be carefull projection, that affect joins, selection conditions afterwards
        if (projection) {
            Operator projectOperator = new ProjectOperator(rootOperator, selectItems);
            rootOperator = projectOperator;
        }
        return rootOperator;
    }


}
