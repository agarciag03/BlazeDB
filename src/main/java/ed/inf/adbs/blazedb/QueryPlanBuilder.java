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

        if (joinOperator && !joinConditions.isEmpty()) { // There is a join condition
            for (Expression joinCondition : joinConditions) {
                // Identify the table on right side to scan it
                BinaryExpression joinExpression = (BinaryExpression) joinCondition;
                Column columnExpression = (Column) joinExpression.getRightExpression();
                Operator scanRightTable = new ScanOperator(columnExpression.getTable().toString());
                rootOperator = new JoinOperator(rootOperator, scanRightTable, joinCondition);
            }
        } else { // There is no join condition, then cross product - BE CAREFUL some could have condition and others not - piazza
            for (Join join : joinsElements) {
                String rightTableName = join.toString();
                Operator joinScanOperator = new ScanOperator(rightTableName);
                rootOperator = new JoinOperator(rootOperator, joinScanOperator, null);
            }

        }

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

}