package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.ProjectOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import ed.inf.adbs.blazedb.operator.Operator;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.FileReader;
import java.util.List;


/**
 * The QueryPlanBuilder class is responsible for building the query plan based on the query
 * Input: Select object
 * Output: Query plan represented as an operator tree ready to be executed
 */

public class QueryPlanBuilder {
    //private Select select = null;
    private PlainSelect plainSelect = null;

    // Method to parse the SQL query - Catch any problem at parsing time
    public void parsingSQL(String inputFile) throws Exception {
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
            //Statement statement = CCJSqlParserUtil.parse("SELECT Student.A  FROM Student WHERE Student.A = 1");


            if (statement != null) {
                Select select = (Select) statement;
                plainSelect = (PlainSelect) select.getSelectBody();

                System.out.println("Statement: " + select);
//                System.out.println("SELECT items: " + select.getPlainSelect().getSelectItems());
//                System.out.println("FROM clause: " + select.getPlainSelect().getFromItem());
//                System.out.println("OTHER TABLES: " + select.getPlainSelect().getJoins());
//                System.out.println("WHERE expression: " + select.getPlainSelect().getWhere());

            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
    }

    // Method to build the query plan based on the query -suitable query plan
    public Operator buildQueryPlan() throws Exception {

        boolean projection = false;
        boolean selection = false;
        Operator rootOperator = null;

        //PlainSelect plainSelect = (PlainSelect) ((Select) statement).getSelectBody();
        //PlainSelect plainSelect = (PlainSelect) this.select.getSelectBody();

        String tableName = plainSelect.getFromItem().toString();
        Expression conditionExpression = plainSelect.getWhere();
        List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();
        //List<SelectItem<?>> selectItems = plainSelect.getSelectItems();

        if (selectItems.size() > 1 ) {
            projection = true;
        } else {
            if (selectItems.get(0).getExpression() instanceof AllColumns) {
                projection = false;
            } else {
                projection = true;
            }
        }

        if (conditionExpression != null) {
            selection = true;
        }

        // Organising the tree of operators
        Operator scanOperator = new ScanOperator(tableName);
        rootOperator = scanOperator;

        if (projection) {
            Operator projectOperator = new ProjectOperator(scanOperator, selectItems);
            rootOperator = projectOperator;
        }
        if (selection) {
            // review later that I can fix this operators I need to find a way to optimise them
            Operator selectOperator = new SelectOperator(rootOperator, conditionExpression);
            rootOperator = selectOperator;
        }


        //Operator selectOperator = new SelectOperator(scanOperator, conditionExpression);
        //Operator projectOperator = new ProjectOperator(selectOperator, selectItems);

        return rootOperator;
    }
}
