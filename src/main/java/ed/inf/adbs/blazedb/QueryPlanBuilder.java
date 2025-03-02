package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.util.Arrays;
import java.util.List;



public class QueryPlanBuilder {

    public static Operator buildQueryPlan(Statement statement) throws Exception {

        // Create a select
        Select select = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        boolean projection = false;
        boolean selection = false;
        boolean join = false;
        boolean distinct = false;
        boolean orderBy = false;
        Operator rootOperator = null;

        //Identifying elements of the query

        String tableName = plainSelect.getFromItem().toString();
        Expression conditionExpression = plainSelect.getWhere();
        List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();
        List<?> joins = plainSelect.getJoins();
        List<?> orderByElements = plainSelect.getOrderByElements();
        //List<SelectItem<?>> selectItems = plainSelect.getSelectItems();


        // Building the tree

        // Review the tree of operators
//        if (selectItems.size() >= 1){
//            if (selectItems.get(0).getExpression() instanceof AllColumns) {
//                projection = false;
//            } else {
//                projection = true;
//            }
//        }
        // Identifying the element in the query
        distinct = plainSelect.getDistinct() != null;

        if (selectItems.size() > 1 || !(selectItems.get(0).getExpression() instanceof AllColumns)) {
                projection = true;
        }

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
                String joinTableName = joinItem.toString();
                Operator joinScanOperator = new ScanOperator(joinTableName);
                Expression joinCondition = conditionExpression; // Simplified for this example
                rootOperator = new JoinOperator(rootOperator, joinScanOperator, joinCondition);
            }
            // Add binarytree to identify the elements of the conditions are tables and are differents to identify a joincondition
        }

        // Be carefull projection, that affect joins, selection conditions afterwards
        if (projection) {
            Operator projectOperator = new ProjectOperator(rootOperator, selectItems);
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
