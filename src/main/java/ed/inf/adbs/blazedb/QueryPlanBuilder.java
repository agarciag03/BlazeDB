package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

/**
 * This class is used to build the query plan for the query that is given as input.
 * In first place, it identifies the elements of the query and then it identifies the operators that are needed to build the query plan.
 * Finally, it builds the query plan based on the operators identified and return it as the root operator of the query plan.
 * The query plan is built in a way that the query can be optimised and executed in the most efficient way.
 */
public class QueryPlanBuilder {

    // Elements of the query
    private Set<String> neededTables = new LinkedHashSet<>(); // All the tables that are needed in the query in the order they appear in the FROM clause
    private Map<Column, Boolean> neededColumns = new HashMap<>();

    private List<Expression> projectionExpressions = new ArrayList<>();
    private List<Function> sumExpressions = new ArrayList<>();
    private List<Expression> selectionExpressions = new ArrayList<>();

    private List<Expression> joinElements = new ArrayList<>();
    private HashSet<Expression> joinExpressions = new LinkedHashSet<>(); // this is used to keep the order of the joins
    private List <Join> crossProductExpressions = new ArrayList<>(); // joins without conditions

    private List<OrderByElement> orderByExpressions = new ArrayList<>();
    private List<Expression> groupByExpressions = new ArrayList<>();
    private Distinct distinctExpression;


    // Flags to identify the operators in the query -  this is used to build the query plan
    private boolean projectionOperator = false;
    private boolean selectionOperator = false;
    private boolean joinOperator = false;
    private boolean distinctOperator = false;
    private boolean orderByOperator = false;
    private boolean groupByOperator = false;
    private boolean sumOperator = false;

    // OPTIMISATION: Flag to identify if the query is always false - Trivial query
    private boolean isAlwaysFalse = false;

    // The root operator of the query plan
    private Operator rootOperator;

    // Method to build the query plan for the query that is given as input
    // optimisation rules are applied to build the query plan in the most efficient way
    public Operator buildQueryPlan(Statement statement) throws Exception {

        // A. Identify the elements of the query that are needed to build the query plan.
        identifyElementsOperators(statement);

        // B. OPTIMISATION: If a trivial query was found (1=2) the program avoids building a query plan
        // that is not needed because the result is empty anyway.
        if (isAlwaysFalse) {
            return null;
        }

        // C. Identify the operators that are needed to build the query plan
        identifyOperators();

        // D. Build the query plan based on the operators identified.
        // Here, RootOperator is the variable that contains the whole tree. To start with, the root operator is reset to null
        rootOperator = null;

        // 1. The program starts scanning the first table that is in FROM clause.
        // OPTIMISATION: In the following method, the program applies also early selections and projections in the table if they are present
        rootOperator = scanWithEarlyOptimisations(neededTables.iterator().next());

        // 2. After scanning and applying early optimisations in the first table, the program will apply join conditions.
        // Having joinExpressions means that there are join conditions in the query
        if (joinOperator && !joinExpressions.isEmpty()) {
            for (Expression joinCondition : joinExpressions) {
                BinaryExpression joinExpression = (BinaryExpression) joinCondition;

                // Identify the table on right side to scanWithEarlyOptimisations
                Column rightcolumn = (Column) joinExpression.getRightExpression();
                rootOperator = new JoinOperator(rootOperator, scanWithEarlyOptimisations(rightcolumn.getTable().toString()), joinCondition);
            }
        }

        // 3. Apply the rest of projections if they are present and also if there are no sum or group by operators
        // If  there are Sum or Groupby operator the projection will be done there.
        if (projectionOperator && !sumOperator && !groupByOperator) {
            rootOperator = new ProjectOperator(rootOperator, projectionExpressions);
        }

        // 4. After applying all the projections, the program will do the cross products if they are present
        // OPTIMISATION: The cross products are done just with the columns needed to reduce the intermediate results
        if (joinOperator && !crossProductExpressions.isEmpty()) {
            for (Join join : crossProductExpressions) {
                // Identify the table on right side to scan it
                String rightTableName = join.toString();
                Operator scanRightTable = scanWithEarlyOptimisations(rightTableName);
                rootOperator = new JoinOperator(rootOperator, scanRightTable, null);
            }
        }

        // 5. Since Groupby and Sum are blocking operators, they are applied almost at the end of the query plan
        if (groupByOperator || sumOperator) {
            rootOperator = new SumOperator(rootOperator, groupByExpressions, sumExpressions, projectionExpressions);
        }


        // 6. Apply the distinct operator if it is present.
        // OPTIMISATION: This operator is applied before OrderBy to reduce the intermediate results that have to be in memory.
        if (distinctOperator) {
            rootOperator = new DuplicateEliminationOperator(rootOperator);
        }

        // 7. Apply the order by operator if it is present, as the last operator in the query plan
        // when all the selections, projections, distinct are applied to reduce the tuples that have to be in memory for sorting.
        if (orderByOperator) {
            rootOperator = new SortOperator(rootOperator, orderByExpressions);
        }
        return rootOperator;
    }

    /**
     * This method is used to identify the elements of the query and the operators that are needed to build the query plan.
     * The results of this method are stored in the attributes of the class (Elements of the query)
     * @param statement The query that is given as input
     */
    public void identifyElementsOperators(Statement statement) {

        // It is assumed that  all the Statements are Selects
        Select select = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        // 1. Identifying the table in the FROM clause and adding it to the neededTables
        neededTables.add(plainSelect.getFromItem().toString());

        // 2. identifying projection (or AllColumns) and SUM Operator in the SELECT clause
        List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();
        if (selectItems != null) {
            identifySelectElements(selectItems);
        }

        // 3. Identifying the selections and joins conditions from the WHERE clause
        Expression whereExpressions = plainSelect.getWhere();
        if (whereExpressions != null) {
            // Here it is need to identify the join conditions and selections going recursively into the AndExpression
            while (whereExpressions instanceof AndExpression) {
                AndExpression andExpr = (AndExpression) whereExpressions;
                identifyWhereExpression(andExpr.getRightExpression());
                whereExpressions = andExpr.getLeftExpression();
            }
            identifyWhereExpression(whereExpressions);
        }

        // 4. Identifying cross products and sorting join conditions in the left tree.
        List<Join> joinsExpressions = plainSelect.getJoins();
        if (joinsExpressions != null) {
            extractJoinConditions(joinsExpressions);
        }

        // 5. Identifying the order by expressions
        List <OrderByElement> orderbyExpressions =  plainSelect.getOrderByElements();
        if (orderbyExpressions != null) {
            orderByExpressions = orderbyExpressions;
            System.out.println("ORDER BY: " + orderbyExpressions);
        }

        // 6. Identify the distinct operator
        distinctExpression = plainSelect.getDistinct();
        if (distinctExpression != null) {
            System.out.println("DISTINCT: " + distinctExpression);
        }

        // 7. Identify the group by operator
        if (plainSelect.getGroupBy() != null) {
            groupByExpressions = plainSelect.getGroupBy().getGroupByExpressionList();
            System.out.println("GROUP BY: " + groupByExpressions);
        }
    }

    /**
     * This method is used to identify the elements in the SELECT clause of the query. The elements that can be identified are: Projections, SUM operations and AllColumns.
     * The elements are identified and stored in the attributes of the class.
     * - projectionExpressions: The columns needed for the projection
     * - sumExpressions: The SUM operations in the query
     * @param selectItems The elements in the SELECT clause of the query
     */
    public void identifySelectElements(List<SelectItem> selectItems) {

        for (SelectItem selectItem : selectItems) {

            if (selectItem.getExpression() instanceof AllColumns) {
                // Identifying AllColumns - No projections needed
                System.out.println("SELECT: AllColumns");

            } else if (selectItem.getExpression() instanceof Column) {
                // Identifying the columns needed for the projection
                projectionExpressions.add(selectItem.getExpression());
                System.out.println("SELECT:  " + selectItem.getExpression());

            } else if (selectItem.getExpression() instanceof Function) {
                // Identifying the SUM operations. SUM is always instance as a Function
                Function function = (Function) selectItem.getExpression();
                if (function.getName().equalsIgnoreCase("SUM")) {
                    sumExpressions.add(function);
                    System.out.println("SUM:  " + function.getParameters());
                }
            } else { // Other functions are not allowed
                System.out.println(" This select function is not allowed: " + selectItem);
            }
        }
    }

    /**
     * This method is used to identify the elements in the WHERE clause of the query. The elements that can be identified are: Selections, trivial queries and Joins.
     * The elements are identified and stored in the attributes of the class:
     * - selectionExpressions: Selections in the query
     * - joinElements: Join conditions in the query
     * - isAlwaysFalse: Flag to identify if the query is always false (Trivial query)
     * @param expression An element in the WHERE clause of the query. This element is extracted from the WHERE clause in a recursive way in the method identifyElementsOperators
     */
    private void identifyWhereExpression(Expression expression) {
        if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;

            // Identifying Joins checking whether the tables involved in the condition are different
            if (isJoinCondition(binaryExpression)) {
                joinElements.add(0, expression);
                System.out.println("JOIN: " + expression);

            } else if(binaryExpression.getLeftExpression() instanceof Column) {
                // If it is not a join condition, but it is a columns is a selection condition
                selectionExpressions.add(expression);
                System.out.println("WHERE: " + expression);

            } else if (binaryExpression.getLeftExpression() instanceof LongValue){
                // Identifying trivial selections and analyse them immediately (OPTIMISATION)
                Integer leftValue = (int) ((LongValue) binaryExpression.getLeftExpression()).getValue();
                Integer rightValue = (int) ((LongValue) binaryExpression.getRightExpression()).getValue();

                if (!leftValue.equals(rightValue)) {
                    System.out.println("TRIVIAL QUERY: " + expression);
                    isAlwaysFalse = true;
                }
                // OPTIMISATION: Trivial query.
                // If the condition is always true (1=1), the program does not consider it.
                // If the condition is always false (1=2), unnecessary operations are avoided turning on the flag isAlwaysFalse.
            }
        }
    }

    /**
     * This method is used to identify the join conditions in the query. Basically, it checks whether the columns involved in the condition are from different tables.
     * @param binaryExpression The join condition that is identified in the query
     * @return True if the condition is a join condition, False otherwise
     */
    private boolean isJoinCondition(BinaryExpression binaryExpression) {

        if (binaryExpression.getLeftExpression() instanceof Column && binaryExpression.getRightExpression() instanceof Column) {
            Column leftColumn = (Column) binaryExpression.getLeftExpression();
            Column rightColumn = (Column) binaryExpression.getRightExpression();

            if (leftColumn.getTable() != null && rightColumn.getTable() != null) {
                String leftTableName = leftColumn.getTable().getName();
                String rightTableName = rightColumn.getTable().getName();

                if (!leftTableName.equals(rightTableName)) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * This method is used to extract the join tables from the query and organise them in the order of the tables in the query. Additionally, it identifies the cross products
     * The elements are identified and stored in the attributes of the class:
     * - neededTables: The tables that are needed in the query in the order they appear in the FROM clause
     * - joinExpressions: The join conditions in the query organised in the order of the tables in the query
     * - crossProductExpressions: The cross products in the query
     * @param joinsExpressions The join conditions that are identified in the query
     */
    private void extractJoinConditions(List<Join> joinsExpressions) {

        // Identifying the order of joins and cross products
        for (Join join : joinsExpressions) {
            neededTables.add(join.toString()); //Identifying the order of join tables in the query

            if(!hasJoinCondition(join.toString())) {
                // identify cross products - Joins without Conditions
                crossProductExpressions.add(join);
                System.out.println("JOIN - CROSS PRODUCT: " + join);
            }
        }

        // Organising the join conditions in the order of the tables in the query
        sortJoinConditions(joinsExpressions);
    }

    /**
     * This method is used to verify if a table has a join condition in the query, in order to identify the cross products.
     * @param tableName The table to be verified
     * @return True if the table has a join condition, False otherwise
     */
    private boolean hasJoinCondition(String tableName) {
        for (Expression joinCondition : joinElements) {

            BinaryExpression joinExpression = (BinaryExpression) joinCondition;
            Column leftExpression = (Column) joinExpression.getLeftExpression();
            Column righExpression = (Column) joinExpression.getRightExpression();

            if (leftExpression.getTable().getName().equals(tableName) || righExpression.getTable().getName().equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method is used to sort the join conditions in the order of the tables in the query.
     * The join conditions are stored in the attributes of the class. The sorted join conditions are stored in the attribute joinExpressions.
     * @param joinsExpressions The join conditions that are identified in the query
     */
    private void sortJoinConditions(List<Join> joinsExpressions) {

        for (String tableName : neededTables) { // sorted tables from left to right

            List<Expression> toRemove = new ArrayList<>();
            List<Expression> toAdd = new ArrayList<>();

            for (Expression joinCondition : joinElements) {
                BinaryExpression joinExpression = (BinaryExpression) joinCondition;

                Column leftExpression = (Column) joinExpression.getLeftExpression();
                Column rightExpression = (Column) joinExpression.getRightExpression();
                String leftTable = leftExpression.getTable().getName();
                String rightTable = rightExpression.getTable().getName();

                // Verifying if the table is involved in the join condition (Right or Left side)
                if (leftTable.equals(tableName) || rightTable.equals(tableName)) {

                    // Verifying if the order is correct. If not, it corrects the expression swapping the tables
                    if (getTableIndex(leftTable) > getTableIndex(rightTable)) {
                        BinaryExpression correctedJoin;

                        if (joinExpression instanceof EqualsTo) {
                            correctedJoin = new EqualsTo(); // equals is the same swapping the tables
                        } else if (joinExpression instanceof NotEqualsTo) {
                            correctedJoin = new NotEqualsTo(); // equals is the same swapping the tables
                        } else if (joinExpression instanceof GreaterThan) {
                            correctedJoin = new MinorThanEquals(); // `>` transforms into `<=`
                        } else if (joinExpression instanceof GreaterThanEquals) {
                            correctedJoin = new MinorThan(); // `>=` transforms into `<`
                        } else if (joinExpression instanceof MinorThan) {
                            correctedJoin = new GreaterThanEquals(); // `<` transforms into `>=`
                        } else { // MinorThanEquals
                            correctedJoin = new GreaterThan(); // `<=` transforms into `>`
                        }

                        correctedJoin.setLeftExpression(rightExpression);
                        correctedJoin.setRightExpression(leftExpression);

                        toRemove.add(joinCondition); // remove the original expression to avoid that it is added twice
                        toAdd.add(correctedJoin);

                    } else {  // If the order is correct
                        toRemove.add(joinCondition);
                        toAdd.add(joinCondition);
                    }
                }
            }
            joinElements.removeAll(toRemove);
            joinExpressions.addAll(toAdd);
        }
    }

    /**
     * This method is used to get the index of a table in the list of tables that are needed in the query.
     * The index is used to sort the join conditions in the order of the tables in the query.
     * @param tableName The table to get the index
     * @return The index of the table in the list of tables that are needed in the query
     */
    private int getTableIndex(String tableName) {
        int index = 0;
        for (String table : neededTables) {
            if (table.equals(tableName)) {
                return index;
            }
            index++;
        }
        return -1; // if the table is not found
    }

    /**
     * This method is used to identify the operators that are needed to build the query plan.
     * The operators are identified based on the elements of the query that were identified before.
     * The results of this method are stored in the attributes of the class (Operators of the query). They will be flags for each operator.
     */
    private void identifyOperators() {

        if (!projectionExpressions.isEmpty() ) {
            projectionOperator = true;
            identifyColumnsForEarlyProjections(); // OPTIMISATION: Identify all the expressions needed in the query to do early projections in the scan operator
        }
        if (!joinExpressions.isEmpty()) {
            joinOperator = true;
        }
        if (!crossProductExpressions.isEmpty()) {
            joinOperator = true;
        }
        if (!selectionExpressions.isEmpty()) {
            selectionOperator = true;
        }
        if (!sumExpressions.isEmpty()) {
            sumOperator = true;
        }
        if (!orderByExpressions.isEmpty()) {
            orderByOperator = true;
        }
        if (distinctExpression != null) {
            distinctOperator = true;
        }
        if (!groupByExpressions.isEmpty()) {
            groupByOperator = true;
        }
    }

    /**
     * This method is used to identify the columns that are needed in the query to do early projections in the scan operator for optimisation.
     * The columns are identified based on the expressions that are needed in the query.
     * The results of the columns needed are stored in the attribute neededColumns.
     */
    private void identifyColumnsForEarlyProjections() {

        // All the expressions that are needed in the query
        List<Expression> expressions = new ArrayList<>();
        expressions.addAll(projectionExpressions);
        expressions.addAll(selectionExpressions);
        expressions.addAll(joinExpressions);
        expressions.addAll(sumExpressions);
        expressions.addAll(groupByExpressions);

        // Note: Not orderby expressions are considered since in the instructions it is mentioned that the attributes mentioned in
        // the ORDER BY are a subset of those retained by the SELECT

        for (Expression expression : expressions) {

            if (expression instanceof Column) {
                // Identify the column needed in each expression
                Column column = (Column) expression;
                neededColumns.put(column, true);

            } else if (expression instanceof Function) {
                // Identify the columns needed in the function SUM.
                Function function = (Function) expression;
                extractColumnsFromFunction(function);

            } else if (expression instanceof BinaryExpression) {
                // Identify the columns needed in the binary expression like selections and joins
                BinaryExpression binaryExpression = (BinaryExpression) expression;

                if (binaryExpression.getLeftExpression() instanceof Column) {
                    Column column = (Column) binaryExpression.getLeftExpression();
                    neededColumns.put(column, true);
                }
                if (binaryExpression.getRightExpression() instanceof Column) {
                    Column column = (Column) binaryExpression.getRightExpression();
                    neededColumns.put(column, true);
                }
            }
        }
    }

    /**
     * This method is used to extract the columns that are needed in the function SUM for the early projections in the scan operator for optimisation.
     * The columns are identified based on the function SUM that is needed in the query.
     * The results of the columns needed are stored in the attribute neededColumns.
     * @param function The function SUM that is needed in the query
     */
    private void extractColumnsFromFunction(Expression function) {
        ExpressionList parameters = ((Function) function).getParameters();
        Expression expression = (Expression) parameters.getExpressions().get(0);

        // If the parameter is a column or part of a multiplication expression we need to extract the columns
        if (expression instanceof Column) {
            Column column = (Column) expression;
            neededColumns.put(column, true);

        } else if (expression instanceof Multiplication) {
            BinaryExpression multiplication = (BinaryExpression) expression;
            extractMultiplicationColumns(multiplication);
        }
    }

    /**
     * This method is used to extract the columns that are needed in the multiplication expression for the early projections in the scan operator for optimisation.
     * This method is built since the way to extract columns in multiplication require using the method recursively when there are expressions like SUM (A*A*B)
     * The columns are identified based on the multiplication expression that is needed in the query.
     * The results of the columns needed are stored in the attribute neededColumns.
     * @param expression The multiplication expression that is needed in the query
     */
    private void extractMultiplicationColumns(Expression expression) {
        BinaryExpression multiplication = (BinaryExpression) expression;
        if (multiplication.getRightExpression() instanceof Column) {
            Column column = (Column) multiplication.getRightExpression();
            neededColumns.put(column, true);
        }
        if (multiplication.getLeftExpression() instanceof Column) {
            Column column = (Column) multiplication.getLeftExpression();
            neededColumns.put(column, true);
        } else {
            // recursive call
            extractMultiplicationColumns(multiplication.getLeftExpression());
        }
    }

    /**
     * This method is used to scan the table and apply early optimisations like selections and projections in the table.
     * The result of this method is stored in the attribute rootOperator.
     * @param fromTable The table to be scanned
     * @return The root operator of the query plan that is built (It can include scan, selection and projection operators)
     * @throws Exception
     */
    private Operator scanWithEarlyOptimisations(String fromTable) throws Exception {

        // 1. Scan the table
        Operator root = new ScanOperator(fromTable);

        // 2. OPTIMISATION: if there is a selection condition for this table, the program will apply it
        if (selectionOperator) {
            for (Expression selectionCondition : selectionExpressions) {
                BinaryExpression binaryExpression = (BinaryExpression) selectionCondition;
                Column column = (Column) binaryExpression.getLeftExpression();
                if (column.getTable().getName().equals(fromTable)){
                    Operator selectOperator = new SelectOperator(root, selectionCondition);
                    root = selectOperator;

                    // After applying the selection, the program will mark this column as not needed in the list of needed columns
                    // It will allow to do early projections in more efficient way, reducing the intermediate results
                    markNoNeededColumn(column);
                }
            }
        }

        // 3. OPTIMISATION: if there are projections in this table, the program will find the columns that are needed in the projection
        // and also the columns needed for the following operators in order to do early projections and reduce intermediate results.
        // If there is not projection, the program will not apply it since all columns are needed
        if (projectionOperator) {
            List<Expression> columnProjection = new ArrayList<>();
            for (Column column : neededColumns.keySet()) {

                // Identify the columns needed for the following operators for this table
                if (column.getTable().getName().equals(fromTable) && neededColumns.get(column)) {
                    columnProjection.add(column);
                }
            }

            // Since in the NeedColumns can be the same column more than once, the program will remove duplicates
            columnProjection = getUniqueColumns(columnProjection);
            // Project only the columns needed
            Operator projectOperator = new ProjectOperator(root, columnProjection);
            root = projectOperator;
        }

        return root;
    }

    /**
     * This method is used to mark the columns that are not needed after one operator is applied.
     * It is built as a for loop with break because the column can be more than once in the list, so the program just marks as not needed the first time it finds it with the value true.
     * The columns are marked as not needed in the attribute neededColumns.
     * @param targetColumn The column to be marked as not needed
     */
    public void markNoNeededColumn(Column targetColumn) {
        for (Map.Entry<Column, Boolean> entry : neededColumns.entrySet()) {
            if (entry.getKey().equals(targetColumn) && entry.getValue()) {
                entry.setValue(false);
                break;
            }
        }
    }

    /**
     * This method is used to get the unique columns for a list. It is important because the query could have the same column in different expressions.
     * The columns are stored in a list and the duplicates are removed.
     * @param columns The list of columns to be checked
     * @return The list of unique columns
     */
    private List<Expression> getUniqueColumns(List<Expression> columns) {
        Set<String> seen = new HashSet<>();
        List<Expression> uniqueColumns = new ArrayList<>();

        for (Expression expr : columns) {
            if (seen.add(expr.toString())) {
                uniqueColumns.add(expr);
            }
        }

        return uniqueColumns;
    }

}