package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static net.sf.jsqlparser.parser.feature.Feature.distinct;


public class QueryPlanBuilder {

    //This method is responsible for parsing the SQL Query and identify all the elements of the query to build the query plan
    public static void parsingSQL(Statement statement) {
        Select select = (Select) statement; // Statement is always a Select
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();


        List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();
        String fromTable = plainSelect.getFromItem().toString(); //the first table in the FROM clause
        //ArrayList <?> joins = (ArrayList<?>) plainSelect.getJoins(); //the remaining tables in joins
        List<Join> joins = plainSelect.getJoins();
        Expression whereExpression = plainSelect.getWhere(); //the condition in the WHERE clause
        Distinct distinct = plainSelect.getDistinct();
        List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
        GroupByElement groupByElements = plainSelect.getGroupBy();


    }
    public static Operator buildQueryPlan(Statement statement) throws Exception {

        parsingSQL(statement);
        // Create a select
        Select select = (Select) statement; // Statement is always a Select
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        boolean projection = false;
        boolean selection = false;
        boolean join = false;
        boolean distinct = false;
        boolean orderBy = false;
        boolean groupBy = false;
        boolean sum = false;
        Operator rootOperator = null;

        //Identifying elements of the query

        String tableName = plainSelect.getFromItem().toString();
        Expression conditionExpression = plainSelect.getWhere();
        List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();
        List<?> joins = plainSelect.getJoins();
        List<?> orderByElements = plainSelect.getOrderByElements();
        //List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        //List<Expression> groupByExpressions = plainSelect.getGroupBy().;
        GroupByElement groupByElements = plainSelect.getGroupBy();


        // Building the tree

        if (groupByElements != null) {
            groupBy = true;
        }


        // Select the columns for projections and for sum
//        ExpressionList sumExpressionList;
//        List<Function> sumExpressions = new ArrayList<>();

//        for (SelectItem selectItem : selectItems) {
//            if (selectItem.getExpression() instanceof Function) {
//                Function function = (Function) selectItem.getExpression();
//                if (function.getName().equals("SUM")) {
//                    sum = true;
//                    sumExpressionList = function.getParameters();
//                    sumExpressions.add(function);
//                }
//            }
//        }

        /// //

        ExpressionList sumExpressionList;
        List<Function> sumExpressions = new ArrayList<>();
        List<SelectItem> projectionItems = new ArrayList<>();
        for (SelectItem selectItem : plainSelect.getSelectItems()) {
            if (selectItem.getExpression() instanceof AllColumns) {
                System.out.println("  - All columns");
                projection = false; // No need to project if all columns are selected

            } else if (selectItem.getExpression() instanceof Function) {
                Function function = (Function) selectItem.getExpression();
                if (function.getName().equalsIgnoreCase("SUM")) {
                    sum = true;
                    sumExpressionList = function.getParameters();
                    sumExpressions.add(function);
                    //ExpressionList params = function.getParameters();
                    System.out.println("  - Función SUM detectada en la columna: " + function.getParameters());

                }
            } else if (selectItem.getExpression() instanceof Column) {
                System.out.println(" projection " + selectItem);
                projectionItems.add(selectItem);
                projection = true;
            } else {
                System.out.println(" This select function is not allowed: " + selectItem);
            }
        }





//
        // Identifying the element in the query
        distinct = plainSelect.getDistinct() != null;

        if (projectionItems.size() != 0) {
            projection = true;
        }
//        if (selectItems.size() > 1 || !(selectItems.get(0).getExpression() instanceof AllColumns)) {
//                projection = true;
//        }

        // Check that selection is not a join condition using binary expression
        if (conditionExpression != null) {
            selection = true;
            if (conditionExpression instanceof BinaryExpression) {
                BinaryExpression binaryExpression = (BinaryExpression) conditionExpression;
                if (binaryExpression.getLeftExpression() instanceof Column && binaryExpression.getRightExpression() instanceof Column) {
                    Column leftColumn = (Column) binaryExpression.getLeftExpression();
                    Column rightColumn = (Column) binaryExpression.getRightExpression();
                    if (leftColumn.getTable() != null && rightColumn.getTable() != null) {
                        String leftTableName = leftColumn.getTable().getName();
                        String rightTableName = rightColumn.getTable().getName();
                        if (leftTableName != null && rightTableName != null && !leftTableName.equals(rightTableName)) {
                            // Hay una tabla en la cláusula WHERE
                            selection = false;
                            join = true;
                        }
                    }
                }
            }
            // Add binarytree to identify the elements of the conditions are tables and are differents to identify a joincondition

        }
        if (joins != null) {
            join = true;
        }

        if (orderByElements != null) {
            orderBy = true;
        }
 /// ///////////////
 /// /////////////// /// /////////////// /// /////////////// /// ///////////////
 /// ///////////////
 /// /////////////// /// /////////////// /// /////////////// /// ///////////////
 /// ///////////////
 /// /////////////// /// /////////////// /// /////////////// /// ///////////////

        // Organising the tree of operators
        Operator scanOperator = new ScanOperator(tableName);
        rootOperator = scanOperator;

        if (selection) {
            // review later that I can fix this operators I need to find a way to optimise them
            // Check that selection is not a join condition using binary expression

            Operator selectOperator = new SelectOperator(rootOperator, conditionExpression);
            rootOperator = selectOperator;
        }
        if (join) {
            for (Object joinItem : joins) {
                String rightTableName = joinItem.toString();
                Operator joinScanOperator = new ScanOperator(rightTableName);
                Expression joinCondition = conditionExpression; // Simplified for this example
                rootOperator = new JoinOperator(rootOperator, joinScanOperator, joinCondition);
            }
            // Add binarytree to identify the elements of the conditions are tables and are differents to identify a joincondition
        }

        // Be carefull projection, that affect joins, selection conditions afterwards
        if (projection) {
            Operator projectOperator = new ProjectOperator(rootOperator, projectionItems);
            rootOperator = projectOperator;
        }

        if (distinct) {
            Operator distinctOperator = new DuplicateEliminationOperator(rootOperator);
            rootOperator = distinctOperator;
        }

        List<Integer> orderByColumn = null;
        if (orderBy) {
            for (Object orderByElement : orderByElements) {
                if (orderByElement instanceof OrderByElement) {
                    OrderByElement element = (OrderByElement) orderByElement;
                    if (element.getExpression() instanceof Column) {
                        Column column = (Column) element.getExpression();
                        String table = column.getTable().getName();
                        String columnName = column.getColumnName();
                        int indexColumn = Catalog.getInstance().getColumnIndex(table, columnName);
                        //adding the indexColumn to the list
                        if (orderByColumn == null) {
                            orderByColumn = orderByColumn = Arrays.asList(indexColumn);
                        } else {
                            orderByColumn.add(indexColumn);
                        }

                        System.out.println("Order by column: " + table + "." + columnName + " " + indexColumn);
                    }
                }
            }
            Operator orderByOperator = new SortOperator(rootOperator, orderByColumn);
            rootOperator = orderByOperator;
        }
        return rootOperator;
    }


}
