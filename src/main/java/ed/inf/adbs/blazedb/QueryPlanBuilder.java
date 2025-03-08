package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryPlanBuilder {

    // Elements of the query
    private String fromTable;
    private List<Expression> projectionExpressions = new ArrayList<>();
    private List<Function> sumExpressions = new ArrayList<>();
    private List<Expression> joinConditions = new ArrayList<>();
    private List<Expression> selectionConditions = new ArrayList<>();
    private List<OrderByElement> orderByElements = new ArrayList<>();
    private List<Expression> groupByElements = new ArrayList<>();
    private Distinct distinctElement;
    private List <Join> joinsElements = new ArrayList<>();

    // Flags to identify the operators
    private boolean projectionOperator = false;
    private boolean selectionOperator = false;
    private boolean joinOperator = false;
    private boolean distinctOperator = false;
    private boolean orderByOperator = false;
    private boolean groupByOperator = false;
    private boolean sumOperator = false;

    /**
     * This method is responsible of identifying all the elements and operators of the query to build the query plan
     *
     * @param statement
     * @throws Exception
     */

    public void identifyElementsOperators(Statement statement) {

        Select select = (Select) statement; // Statement is always a Select
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        // 0. Identifying the table in the FROM clause
        fromTable = plainSelect.getFromItem().toString();

        // 1. identifying projection (or AllColumns) and SUM Operator
        List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (selectItem.getExpression() instanceof AllColumns) {
                System.out.println("SELECT *");
                projectionOperator = false; // No need to project if all columns are selected

            } else if (selectItem.getExpression() instanceof Function) {
                // sum is always instance as a Function
                Function function = (Function) selectItem.getExpression();
                if (function.getName().equalsIgnoreCase("SUM")) {
                    sumExpressions.add(function);
                    sumOperator = true;
                    System.out.println("SUM:  " + function.getParameters());
                }
            } else if (selectItem.getExpression() instanceof Column) {
                // selectItem could be a number or a column
                projectionExpressions.add(selectItem.getExpression());
                projectionOperator = true;
                System.out.println("SELECT:  " + selectItem.getExpression());

            } else {
                System.out.println(" This select function is not allowed: " + selectItem);
            }
        }

        // 2. Identifying the selections and joins operators
        Expression whereExpressions = plainSelect.getWhere(); //the condition in the WHERE clause
        if (whereExpressions != null) {
            // Here we need to identify the join conditions and selections going recursively into the AndExpression
            while (whereExpressions instanceof AndExpression) {
                AndExpression andExpr = (AndExpression) whereExpressions;
                identifyWhereExpression(andExpr.getRightExpression());
                whereExpressions = andExpr.getLeftExpression();
            }
            identifyWhereExpression(whereExpressions);
        }

        // 3. Identifying the joins operator without any condition
        List<Join> joinsExpressions = plainSelect.getJoins();
        if (joinsExpressions != null) {
            System.out.println("JOIN: " + joinsExpressions);
            joinsElements = joinsExpressions;
        }

        // 4. Identifying the order by
        List <OrderByElement> orderbyExpressions =  plainSelect.getOrderByElements();
        if (orderbyExpressions != null) {
            System.out.println("ORDER BY: " + orderbyExpressions);
            orderByElements = orderbyExpressions;
        }

        // 5. Identify the distinct operator
        distinctElement = plainSelect.getDistinct();
        System.out.println("DISTINCT: " + distinctElement);

        // 6. Identify the group by operator
       GroupByElement groupByExpressions = plainSelect.getGroupBy();
        if (groupByExpressions != null) {
            System.out.println("GROUP BY: " + groupByExpressions);
            groupByElements = groupByExpressions.getGroupByExpressions();
        }

    }

    private void identifyWhereExpression(Expression expression) {
        if (expression instanceof BinaryExpression) {
            if (isJoinCondition((BinaryExpression) expression)) {
                this.joinConditions.add(expression);
                System.out.println("JOIN: " + expression);
            } else {
                this.selectionConditions.add(expression);
                System.out.println("WHERE: " + expression);
            }
        }
    }

    private static boolean isJoinCondition(BinaryExpression binaryExpression) {

        if (binaryExpression.getLeftExpression() instanceof Column && binaryExpression.getRightExpression() instanceof Column) {
            Column leftColumn = (Column) binaryExpression.getLeftExpression();
            Column rightColumn = (Column) binaryExpression.getRightExpression();

            if (leftColumn.getTable() != null && rightColumn.getTable() != null) {
                String leftTableName = leftColumn.getTable().getName();
                String rightTableName = rightColumn.getTable().getName();

                if (leftTableName != null && rightTableName != null && !leftTableName.equals(rightTableName)) {
                    return true;
                } else {
                    // In this case it is a comparison between columns of the same table
                    return false;
                }
            }
        }
        return false;
    }

    private void checkOperators() {

        if (!this.projectionExpressions.isEmpty()) {
            this.projectionOperator = true;
        }
        if (!this.joinConditions.isEmpty()) {
            this.joinOperator = true;
        }
        if (!this.selectionConditions.isEmpty()) {
            this.selectionOperator = true;
        }
        if (!this.sumExpressions.isEmpty()) {
            this.sumOperator = true;
        }
        if (!this.orderByElements.isEmpty()) {
            this.orderByOperator = true;
        }
        if (this.distinctElement != null) {
            this.distinctOperator = true;
        }
        if (!this.joinsElements.isEmpty()) {
            this.joinOperator = true;
        }
        if (!this.groupByElements.isEmpty()) {
            this.groupByOperator = true;
        }
    }

    public Operator buildQueryPlan(Statement statement) throws Exception {
        identifyElementsOperators(statement);
        checkOperators();

        Operator rootOperator = null;

        // 1. If we only have from table without any other operator
        Operator scanOperator = new ScanOperator(fromTable);
        rootOperator = scanOperator;

        // 2. Adding the selection operator to optimise the query
        if (selectionOperator) {
            for (Expression selectionCondition : selectionConditions) {
                Operator selectOperator = new SelectOperator(scanOperator, selectionCondition);
                rootOperator = selectOperator;
            }
        }

        // Evaluate when there is no condition as cross product
        if (joinOperator) {
            for (Expression joinCondition : joinConditions) {
                // Identify the table on right side to scan it
                BinaryExpression joinExpression = (BinaryExpression) joinCondition;
                Column columnExpression = (Column) joinExpression.getRightExpression();
                Operator scanRightTable = new ScanOperator(columnExpression.getTable().toString());
                rootOperator = new JoinOperator(rootOperator, scanRightTable, joinCondition);
            }
        }


//        if (join) {
//            for (Object joinItem : joins) {
//                String rightTableName = joinItem.toString();
//                Operator joinScanOperator = new ScanOperator(rightTableName);
////                BinaryExpression joinCondition;
//
//                if (joinConditions.size() > 0) {
//                    for (Expression joinCondition : joinConditions) {
//                        rootOperator = new JoinOperator(rootOperator, joinScanOperator, (BinaryExpression) joinConditions.get(0));
//                    }
//                } else {
//                    rootOperator = new JoinOperator(rootOperator, joinScanOperator, null);
//                }
//            }








        // Last step: Projections - Be carefull projection, that affect joins, selection conditions afterwards
        if (projectionOperator) {
            Operator projectOperator = new ProjectOperator(rootOperator, projectionExpressions);
            rootOperator = projectOperator;
        }

        // Be carefull, keep distinct at the end of the operators
        if (distinctOperator) {
            Operator distinctOperator = new DuplicateEliminationOperator(rootOperator);
            rootOperator = distinctOperator;
        }

        // Order by each element that we have in the list
        if (orderByOperator) {
            for (OrderByElement orderByElement : orderByElements) {
                Expression orderByColumn = orderByElement.getExpression();
                Operator orderByOperator = new SortOperator(rootOperator, orderByColumn);
                rootOperator = orderByOperator;
            }
        }

        return rootOperator;

    }

    public Operator buildQueryPlan2(Statement statement) throws Exception {

        identifyElementsOperators(statement);
        checkOperators();


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

        /// // identifying sums, projections and allcolumns

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
        // Identifying joins and selections
        List<Expression> joinConditions = new ArrayList<>();
        List<Expression> selectionConditions = new ArrayList<>();


        if (conditionExpression != null) {
            selection = true;
//            if (conditionExpression instanceof BinaryExpression) {
//                BinaryExpression binaryExpression = (BinaryExpression) conditionExpression;
//
//                // Identify AND Expression
//
//                if (binaryExpression.getLeftExpression() instanceof Column && binaryExpression.getRightExpression() instanceof Column) {
//                    Column leftColumn = (Column) binaryExpression.getLeftExpression();
//                    Column rightColumn = (Column) binaryExpression.getRightExpression();
//
//                    if (leftColumn.getTable() != null && rightColumn.getTable() != null) {
//                        String leftTableName = leftColumn.getTable().getName();
//                        String rightTableName = rightColumn.getTable().getName();
//
//                        if (leftTableName != null && rightTableName != null && !leftTableName.equals(rightTableName)) {
//                            // Hay una tabla en la cláusula WHERE
//                            selection = false;
//                            joinConditions = conditionExpression;
//                            join = true;
//
//                        } else {
//                            selection = true;
//                        }
//                    }
//                }
//            }

            if (conditionExpression instanceof AndExpression) {
                while (conditionExpression instanceof AndExpression) {
                    AndExpression andExpr = (AndExpression) conditionExpression;
                    Expression leftExpression = andExpr.getLeftExpression();
                    Expression rightExpression = andExpr.getLeftExpression();

                    if (conditionExpression instanceof BinaryExpression) {
                        BinaryExpression binaryExpression = (BinaryExpression) conditionExpression;

                        if (isJoinCondition(binaryExpression)) {
                            join = true;
                            joinConditions.add(conditionExpression);
                        } else {
                            selection = true;
                            selectionConditions.add(conditionExpression);
                        }
                    }
                    conditionExpression = rightExpression;

                }
            } else {
                if (conditionExpression instanceof BinaryExpression) {
                    BinaryExpression binaryExpression = (BinaryExpression) conditionExpression;

                    if (isJoinCondition(binaryExpression)) {
                        join = true;
                        selection = false;
                        joinConditions.add(conditionExpression);
                    } else {
                        selection = true;
                        selectionConditions.add(conditionExpression);
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

        // ensayo

        // Organising the tree of operators
        Operator scanOperator = new ScanOperator(tableName);
        rootOperator = scanOperator;

        if (selection) {
            // review later that I can fix this operators I need to find a way to optimise them
            // Check that selection is not a join condition using binary expression

            Operator selectOperator = new SelectOperator(rootOperator, conditionExpression);
            rootOperator = selectOperator;
        }


        //Identifying the Join.First identify the where condition there
        if (join) {
            for (Object joinItem : joins) {
                String rightTableName = joinItem.toString();
                Operator joinScanOperator = new ScanOperator(rightTableName);
//                BinaryExpression joinCondition;

                if (joinConditions.size() > 0) {
                    for (Expression joinCondition : joinConditions) {
                        rootOperator = new JoinOperator(rootOperator, joinScanOperator, (BinaryExpression) joinConditions.get(0));
                    }
                } else {
                    rootOperator = new JoinOperator(rootOperator, joinScanOperator, null);
                }
            }

//            if (joinConditions.size() > 0) { // There is a clause in where with join
//                for (Expression joinCondition : joinConditions) {
//                    BinaryExpression binaryExpression = (BinaryExpression) conditionExpression;
//                    // Identify the table to join for everychild in the join
//                    Operator joinScanOperator = new ScanOperator(joinCondition.toString());
//                    rootOperator = new JoinOperator(rootOperator, joinScanOperator, (BinaryExpression) joinConditions.get(0));
//                }
//                // Identify the table to join for everychild in the join
//
//            }

        }
            // Add binarytree to identify the elements of the conditions are tables and are differents to identify a joincondition

        // Be carefull projection, that affect joins, selection conditions afterwards
        if (projection) {
            //Operator projectOperator = new ProjectOperator(rootOperator, projectionItems);
            //rootOperator = projectOperator;
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
            //Operator orderByOperator = new SortOperator(rootOperator, orderByColumn);
            //rootOperator = orderByOperator;
        }

        if (sum) {
            Operator sumOperator = new SumOperator(rootOperator, groupByElements, sumExpressions);
            rootOperator = sumOperator;
        }

        return rootOperator;
    }



}