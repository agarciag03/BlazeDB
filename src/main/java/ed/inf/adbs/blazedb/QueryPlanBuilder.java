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

public class QueryPlanBuilder {

    // Elements of the query
    private Set<String> neededTables = new LinkedHashSet<>();
    private Map<Column, Boolean> neededColumns = new HashMap<>();

    private List<Expression> projectionExpressions = new ArrayList<>();
    private List<Function> sumExpressions = new ArrayList<>();

    private List<Expression> joinElements = new ArrayList<>();
    private HashSet<Expression> joinExpressions = new LinkedHashSet<>();
    private List <Join> crossProductExpressions = new ArrayList<>();

    private List<Expression> selectionExpressions = new ArrayList<>();

    private List<OrderByElement> orderByExpressions = new ArrayList<>();
    private List<Expression> groupByExpressions = new ArrayList<>();
    private Distinct distinctExpression;


    // Flags to identify the operators
    private boolean projectionOperator = false;
    private boolean selectionOperator = false;
    private boolean joinOperator = false;
    private boolean distinctOperator = false;
    private boolean orderByOperator = false;
    private boolean groupByOperator = false;
    private boolean sumOperator = false;

    // Flag to identify if the query is always false
    private boolean isAlwaysFalse = false;

    // The root operator of the query plan
    private Operator rootOperator;

    public void identifyElementsOperators(Statement statement) {

        // It is assumed that  all the Statements are Selects
        Select select = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        // 0. Identifying the table in the FROM clause
        //String fromTable = plainSelect.getFromItem().toString();
        //neededTables.put(plainSelect.getFromItem().toString(), false);
        neededTables.add(plainSelect.getFromItem().toString());
        // 1. identifying projection (or AllColumns) and SUM Operator
        // list of SelectItem objects, where each one is either AllColumns
        //(for a SELECT * ) or a SelectExpressionItem.
        List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (selectItem.getExpression() instanceof AllColumns) {
                System.out.println("SELECT: AllCollumns"); // no projections needed

            } else if (selectItem.getExpression() instanceof Column) {
                // SelectExpressionItem will always be a Column
                // selectItem could be a number or a column
                projectionExpressions.add(selectItem.getExpression());
                System.out.println("SELECT:  " + selectItem.getExpression());

            } else if (selectItem.getExpression() instanceof Function) {
                // sum is always instance as a Function

                Function function = (Function) selectItem.getExpression();
                if (function.getName().equalsIgnoreCase("SUM")) {

                    Expression sumExpression = function.getParameters();
                    //sumExpressions.add(sumExpression);
                    sumExpressions.add(function);//selectItem.getExpression());

                    System.out.println("SUM:  " + function.getParameters());
                }
            } else {
                System.out.println(" This select function is not allowed: " + selectItem);
            }
        }

        // strategy for extracting join conditions from the WHERE clause and evaluating
        //them as part of the join

        // 2. Identifying the selections and joins conditions
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

        // 3. Extract JoinsElements
        List<Join> joinsExpressions = plainSelect.getJoins();
        if (joinsExpressions != null) {
            for (Join join : joinsExpressions) {
                //neededTables.put(join.toString(), false); // Identifying the tables in the query
                neededTables.add(join.toString());
                // identify Joins without Conditions - Join tables without conditions are cross products
                if(!hasJoinCondition(join.toString())) {
                    System.out.println("JOIN - CROSS PRODUCT: " + join);
                    crossProductExpressions.add(join);
                } else {

                    System.out.println("organise the join conditions in left tree");
                }
            }
        }

        // Organise the join conditions in the left tree and in right order of conditions.

        // Implementation #5
        if (joinsExpressions != null) { // Hay expresiones de JOIN
            for (String tableName : neededTables) { // Tablas en orden de izquierda a derecha

                List<Expression> toRemove = new ArrayList<>();
                List<Expression> toAdd = new ArrayList<>();

                for (Expression joinCondition : joinElements) {
                    BinaryExpression joinExpression = (BinaryExpression) joinCondition;
                    Column leftExpression = (Column) joinExpression.getLeftExpression();
                    Column rightExpression = (Column) joinExpression.getRightExpression();

                    String leftTable = leftExpression.getTable().getName();
                    String rightTable = rightExpression.getTable().getName();

                    // Verifica si la condición de JOIN involucra la tabla actual
                    if (leftTable.equals(tableName) || rightTable.equals(tableName)) {

                        // Verifica si el orden está incorrecto

                            if (neededTables.contains(rightTable) && neededTables.contains(leftTable)
                                    && getTableIndex(leftTable) > getTableIndex(rightTable)) {

                            // Corrige la expresión según el tipo de operador
                            BinaryExpression correctedJoin;
                            if (joinExpression instanceof EqualsTo) { // equals is the same swapping the columns
                                correctedJoin = new EqualsTo();
                            } else if (joinExpression instanceof NotEqualsTo) {
                                correctedJoin = new NotEqualsTo();
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

                            toRemove.add(joinCondition);
                            toAdd.add(correctedJoin);

                        } else { // Si el orden es correcto
                            toRemove.add(joinCondition);
                            toAdd.add(joinCondition);
                        }
                    }
                }

                // **Eliminar elementos después de la iteración**
                joinElements.removeAll(toRemove);
                joinExpressions.addAll(toAdd);
            }
        }


                    /// ///////

        // 4. Identifying the order by expressions
        List <OrderByElement> orderbyExpressions =  plainSelect.getOrderByElements();
        if (orderbyExpressions != null) {
            System.out.println("ORDER BY: " + orderbyExpressions);
            orderByExpressions = orderbyExpressions;
        }

        // 5. Identify the distinct operator
        distinctExpression = plainSelect.getDistinct();
        if (distinctExpression != null) {
            System.out.println("DISTINCT: " + distinctExpression);
        }

        // 6. Identify the group by operator
        if (plainSelect.getGroupBy() != null) {
            groupByExpressions = plainSelect.getGroupBy().getGroupByExpressionList();
            System.out.println("GROUP BY: " + groupByExpressions);
        }


    }

    private int getTableIndex(String tableName) {
        int index = 0;
        for (String table : neededTables) {
            if (table.equals(tableName)) {
                return index;
            }
            index++;
        }
        return -1; // No encontrado
    }

    // this method is used to identify the columns when there is a projection operator. It is useful for early projections for optimisation
    private void identifyExpressions() {

        List<Expression> expressions = new ArrayList<>();

        expressions.addAll(projectionExpressions);
        expressions.addAll(selectionExpressions);
        expressions.addAll(joinExpressions);
        expressions.addAll(sumExpressions);
        expressions.addAll(groupByExpressions);

        // Not orderby expressions are considered since in the instructions it is mentioned that the attributes mentioned in
        // the ORDER BY are a subset of those retained by the SELECT

        for (Expression expression : expressions) {

            if (expression instanceof Column) {
                // Identify the column needed in each expression
                Column column = (Column) expression;
                 neededColumns.put(column, true);

            } else if (expression instanceof Function) {
                // Identify the columns needed in the function SUM
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

    // Method to extract the columns needed in the function SUM
    private void extractColumnsFromFunction(Expression function) {
        ExpressionList parameters = ((Function) function).getParameters();
        Expression expression = (Expression) parameters.getExpressions().get(0);

        // If the parameter is a column or part of a multiplication expression we need to extract the columns for the Optimisation: early projection
        if (expression instanceof Column) {
            Column column = (Column) expression;
            neededColumns.put(column, true);

        } else if (expression instanceof Multiplication) { // multiplication
            BinaryExpression multiplication = (BinaryExpression) expression;
            getMultiplicationParameters(multiplication);
        }
    }

    // to extract the columns needed in the multiplication expression in a recursive way
    private void getMultiplicationParameters (Expression expression) {
        BinaryExpression multiplication = (BinaryExpression) expression;
        if (multiplication.getRightExpression() instanceof Column) {
            Column column = (Column) multiplication.getRightExpression();
            neededColumns.put(column, true);
        }
        if (multiplication.getLeftExpression() instanceof Column) {
            Column column = (Column) multiplication.getLeftExpression();
            neededColumns.put(column, true);
        } else { // recursive call
            getMultiplicationParameters(multiplication.getLeftExpression());
        }
    }


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

    private void identifyWhereExpression(Expression expression) {
        if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;

            // Identify Joins
            if (isJoinCondition(binaryExpression)) {
                //this.joinConditions.add(expression);
                this.joinElements.add(0, expression);
                System.out.println("JOIN: " + expression);
//            } else {
//
//                this.selectionConditions.add(expression);
//
//                //new implementation
//                  if (binaryExpression.getLeftExpression() instanceof Column) {
//                    Column column = (Column) binaryExpression.getLeftExpression();
//                    String columnName = column.getTable().getName();
//                    selectionColumns.put(columnName, expression);
//                } else {
//                    // no columns - long values
//                    selectionColumns.put("-1", expression);
//                }
//                System.out.println("WHERE: " + expression);
//            }
            } else if(binaryExpression.getLeftExpression() instanceof Column) {
                // Identify Selections
                Column column = (Column) binaryExpression.getLeftExpression();
                String columnName = column.getTable().getName();
                //selectionColumns.put(columnName, expression);
                this.selectionExpressions.add(expression);

            } else if (binaryExpression.getLeftExpression() instanceof LongValue){
                // Identify trivial selections and analyse them

                Integer leftValue = (int) ((LongValue) binaryExpression.getLeftExpression()).getValue();
                Integer rightValue = (int) ((LongValue) binaryExpression.getRightExpression()).getValue();

                if (leftValue != rightValue) {
                    // Trivial query
                    System.out.println("Trivial query: " + expression);
                    isAlwaysFalse = true;
                }

                // Note: If the condition  is always true the program does not consider it
                //this.selectionExpressions.add(expression);

                //OPTIMISATION: Trivial query. Here we want to avoid to avoid operation that are not needed in the query, in case of 1 = 2

                System.out.println("No guardé WHERE: " + expression);
            }
        }
    }

    private boolean isJoinCondition(BinaryExpression binaryExpression) {

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

    private void identifyOperators() {

        if (!this.projectionExpressions.isEmpty() ) {
            this.projectionOperator = true;
            identifyExpressions(); // OPTIMISATION: This step will allow to do early projections
        }
        if (!this.joinElements.isEmpty() || !this.joinExpressions.isEmpty()) {
            this.joinOperator = true;
        }
        if (!this.crossProductExpressions.isEmpty()) {
            this.joinOperator = true;
        }
        if (!this.selectionExpressions.isEmpty()) {
            this.selectionOperator = true;
        }
        if (!this.sumExpressions.isEmpty()) {
            this.sumOperator = true;
        }
        if (!this.orderByExpressions.isEmpty()) {
            this.orderByOperator = true;
        }
        if (this.distinctExpression != null) {
            this.distinctOperator = true;
        }
        if (!this.groupByExpressions.isEmpty()) {
            this.groupByOperator = true;
        }
    }


    /// ////////////////////////////////////
    /// ////////////////////////////////////
    /// ////////////////////////////////////



    public Operator buildQueryPlan(Statement statement) throws Exception {


        identifyElementsOperators(statement);

        // OPTIMISATION: Trivial query. Here we want to avoid to avoid operation that are not needed in the query, in case of 1 = 2
        if (isAlwaysFalse) {
            return null;
        }


        identifyOperators();


        // Building a tree of operators. RootOperator is the variable that contains the whole tree.
        // Reset the root operator to start with the query plan
        rootOperator = null;

        // 1. Scan the first table that we have in the FROM clause
        // 2. If there is any selection condition for this table, apply it
        rootOperator = scanRelation(neededTables.iterator().next());

        // 4. After having all the selections applied, we can make the joins
        // Joins should do it in order based on Joins in the query. DONE
//        if (joinOperator && !joinConditions.isEmpty()) { // There is a join condition
//            for (Expression joinCondition : joinConditions) {
        if (joinOperator && !joinExpressions.isEmpty()) { // There is a join condition
            for (Expression joinCondition : joinExpressions) {
                BinaryExpression joinExpression = (BinaryExpression) joinCondition;

                // Identify the table on right side to scan it
                Column rightcolumn = (Column) joinExpression.getRightExpression();
                rootOperator = new JoinOperator(rootOperator, scanRelation(rightcolumn.getTable().toString()), joinCondition);
            }
        }


        // Last step: Projections - Be carefull projection, that affect joins, selection conditions afterwards
        // if there is sum or group by , they will send the result to the projection

        // Should I use the projection by table before Joins?
        // We use projections if neither sum nor group by are present, otherwise, the projection will be done in the SumOperator
        if (projectionOperator && !sumOperator && !groupByOperator) {
            Operator projectOperator = new ProjectOperator(rootOperator, projectionExpressions);
            rootOperator = projectOperator;
        }

        // Cross products without conditions
        if (joinOperator && !crossProductExpressions.isEmpty()) {
            for (Join join : crossProductExpressions) {
                // Identify the table on right side to scan it
                String rightTableName = join.toString();
                Operator scanRightTable = scanRelation(rightTableName);
                rootOperator = new JoinOperator(rootOperator, scanRightTable, null);
            }
        }

        // blocking Operato
        if (groupByOperator || sumOperator) {
            Operator groupByOperator = new SumOperator(rootOperator, groupByExpressions, sumExpressions, projectionExpressions);
            rootOperator = groupByOperator;
        }


        // Be carefull, keep distinct at the end of the operators
        if (distinctOperator) {
            Operator distinctOperator = new DuplicateEliminationOperator(rootOperator);
            rootOperator = distinctOperator;
        }


        // we can assume that order operator can be after projection.- It is good to delay sorting as late as possible, in
        //particular to do it after the projection(s), because there will be less data to sort that way.
        if (orderByOperator) {
            Operator orderByOperator = new SortOperator(rootOperator, orderByExpressions);
            rootOperator = orderByOperator;
        }

        return rootOperator;
    }


    // non-optional scan operator
    private Operator scanRelation(String fromTable) throws Exception {

        // 1. Scan Table
        Operator root = new ScanOperator(fromTable);

        // OPTIMISATION: if there is a selection condition for this table, apply it
        // Obviously it is most efficient to evaluate the selections as
        //early as possible

        if (selectionOperator) {
            for (Expression selectionCondition : selectionExpressions) {
                BinaryExpression binaryExpression = (BinaryExpression) selectionCondition;
                Column column = (Column) binaryExpression.getLeftExpression();
                if (column.getTable().getName().equals(fromTable)){
                    Operator selectOperator = new SelectOperator(root, selectionCondition);
                    root = selectOperator;
                    markNoNeededColumn(column);
                }
            }
        }

        // OPTIMISATION: if there are projections in the select we could apply including the columns that are needed for the subsequent operators. It is a way to reduce the intermediate results
        // Check if which columns are needed for the following operators and just keep them

        if (projectionOperator) {
            List<Expression> columnProjection = new ArrayList<>();
            for (Column column : neededColumns.keySet()) {

                // Identify the columns needed for the following operators for this table
                if (column.getTable().getName().equals(fromTable) && neededColumns.get(column)) {
                    columnProjection.add(column);
                }
            }

            columnProjection = getUniqueColumns(columnProjection); // this method is used to remove duplicates
            Operator projectOperator = new ProjectOperator(root, columnProjection); // project all columns that I need
            root = projectOperator;
        }

        return root;
    }


    // Method to get the unique columns for a list. It is important because the query could have the same column in different expressions
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

    // Method to mark the columns that are not needed after one operator is applied
    public void markNoNeededColumn(Column targetColumn) {
        for (Map.Entry<Column, Boolean> entry : neededColumns.entrySet()) {
            if (entry.getKey().equals(targetColumn) && entry.getValue()) {
                entry.setValue(false);
                break;
            }
        }
    }



}