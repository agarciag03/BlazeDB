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
    //private String fromTable;
    private Map<String, Boolean> queryTables = new LinkedHashMap<>();
    private Map<String, HashSet<String>> tableColumnsMentioned = new HashMap<>();

    private List<Expression> projectionExpressions = new ArrayList<>();
    private List<Function> sumExpressions = new ArrayList<>();

    private List<Expression> joinConditions = new ArrayList<>();
    private HashSet<Expression> joinElements = new LinkedHashSet<>();
    private List <Join> crossProductElements = new ArrayList<>();

    private Map<String, Expression> selectionColumns = new HashMap<>();
    private List<Expression> selectionConditions = new ArrayList<>();

    private List<OrderByElement> orderByElements = new ArrayList<>();
    private List<Expression> groupByElements = new ArrayList<>();
    private Distinct distinctElement;


    // Flags to identify the operators
    private boolean projectionOperator = false;
    private boolean selectionOperator = false;
    private boolean joinOperator = false;
    private boolean distinctOperator = false;
    private boolean orderByOperator = false;
    private boolean groupByOperator = false;
    private boolean sumOperator = false;

    private Operator rootOperator;

    /**
     * This method is responsible of identifying all the elements and operators of the query to build the query plan
     *
     * @param statement
     * @throws Exception
     */
    public void identifyElementsOperators(Statement statement) {

        // It is assumed that  all the Statements are Selects
        Select select = (Select) statement; // Statement is always a Select
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        // 0. Identifying the table in the FROM clause
        //String fromTable = plainSelect.getFromItem().toString();
        queryTables.put(plainSelect.getFromItem().toString(), false);

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
                queryTables.put(join.toString(), false); // Identifying the tables in the query

                // identify Joins without Conditions - Join tables without conditions are cross products
                if(!hasJoinCondition(join.toString())) {
                    System.out.println("JOIN - CROSS PRODUCT: " + join);
                    crossProductElements.add(join);
                } else {

                    System.out.println("organise the join conditions in left tree");
                }
            }
        }

        // Organise the join conditions in the left tree and in right order of conditions.

        // Implementation #5
        if (joinsExpressions != null) { // Hay expresiones de JOIN
            for (String tableName : queryTables.keySet()) { // Tablas en orden de izquierda a derecha

                List<Expression> toRemove = new ArrayList<>();
                List<Expression> toAdd = new ArrayList<>();

                for (Expression joinCondition : joinConditions) {
                    BinaryExpression joinExpression = (BinaryExpression) joinCondition;
                    Column leftExpression = (Column) joinExpression.getLeftExpression();
                    Column rightExpression = (Column) joinExpression.getRightExpression();

                    String leftTable = leftExpression.getTable().getName();
                    String rightTable = rightExpression.getTable().getName();

                    // Verifica si la condición de JOIN involucra la tabla actual
                    if (leftTable.equals(tableName) || rightTable.equals(tableName)) {

                        // Verifica si el orden está incorrecto
                        if (queryTables.containsKey(rightTable) && queryTables.containsKey(leftTable)
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
                joinConditions.removeAll(toRemove);
                joinElements.addAll(toAdd);
            }
        }


                    /// ///////

        // 4. Identifying the order by expressions
        List <OrderByElement> orderbyExpressions =  plainSelect.getOrderByElements();
        if (orderbyExpressions != null) {
            System.out.println("ORDER BY: " + orderbyExpressions);
            orderByElements = orderbyExpressions;
        }

        // 5. Identify the distinct operator
        distinctElement = plainSelect.getDistinct();
        if (distinctElement != null) {
            System.out.println("DISTINCT: " + distinctElement);
        }

        // 6. Identify the group by operator
        if (plainSelect.getGroupBy() != null) {
            groupByElements = plainSelect.getGroupBy().getGroupByExpressionList();
            System.out.println("GROUP BY: " + groupByElements);
        }



    }

    private int getTableIndex(String tableName) {
        int index = 0;
        for (String table : queryTables.keySet()) {
            if (table.equals(tableName)) {
                return index;
            }
            index++;
        }
        return -1; // No encontrado
    }

    private void identifyExpressions() {

        //About Orderby: You may also assume that the attributes mentioned in the ORDER BY are a subset of
        //those retained by the SELECT.  So we don't need to consider the columns in the ORDER BY clause, projection is enough. A query like SELECT
        //S.A FROM S ORDER BY S.B is valid SQL, but we choose not to support it in this project.

        List<Expression> expressions = new ArrayList<>();
        expressions.addAll(projectionExpressions);
        expressions.addAll(selectionConditions);
        // check there is not elements here to identify the projections
        expressions.addAll(joinConditions);
        //expressions.add((Expression) joinElements);
        expressions.addAll(sumExpressions);
        //expressions.addAll(orderByElements);
        expressions.addAll(groupByElements);

        for (Expression expression : expressions) {

            if (expression instanceof Column) {
                Column column = (Column) expression;
                tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.toString());

            } else if (expression instanceof Function) {
                Function function = (Function) expression;
                extractColumnsFromFunction(function);

            } else if (expression instanceof BinaryExpression) {
                BinaryExpression binaryExpression = (BinaryExpression) expression;
                if (binaryExpression.getLeftExpression() instanceof Column) {
                    Column column = (Column) binaryExpression.getLeftExpression();
                    tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.toString());

                } else if (binaryExpression.getRightExpression() instanceof Column) {
                    Column column = (Column) binaryExpression.getRightExpression();
                    tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.toString());

                }
            }
        }
    }

    private void extractColumnsFromFunction(Expression function) {
        List<String> columnNames = new ArrayList<>();
        //ExpressionList parameters = function.getParameters();

        ExpressionList parameters = ((Function) function).getParameters();

        if (parameters != null && parameters.getExpressions().size() == 1) {
            Expression param = (Expression) parameters.getExpressions().get(0);

            if (param instanceof LongValue) { // Sum by constant
                //do nothing
                // return (int) ((LongValue) param).getValue();

            } else if (param instanceof Column) { // Sum by column
                Column column = (Column) param;
                tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.toString());

            } else if (param instanceof BinaryExpression) { // sum multiplication
                //return multiplicacionExpression((BinaryExpression) param, tuple); // SUM(A * B)
                if (param instanceof Column) {
                    // Si la expresión es una columna, obtenemos su valor del tuple
                    Column column = (Column) param;
                    tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.toString());

                } else if (param instanceof LongValue) {
                    // Si la expresión es un valor numérico (ej. 5, 10, etc.)
                    //Do nothing
                } else if (param instanceof Multiplication) {
                    // Si es una multiplicación, descomponemos en izquierda y derecha recursivamente
                    BinaryExpression multiplication = (BinaryExpression) param;
                    getMultiplicationParameters(multiplication);
//                    if (multiplication.getRightExpression() instanceof Column) {
//                        Column column = (Column) multiplication.getRightExpression();
//                        tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.getColumnName());
//                    }
//                    if (multiplication.getLeftExpression() instanceof Column) {
//                        Column column = (Column) multiplication.getLeftExpression();
//                        tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.getColumnName());
//                    } else {
//                        extractColumnsFromFunction(multiplication.getLeftExpression());
//                    }
                    //tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.getColumnName());

                    //extractColumnsFromFunction(multiplication);
                    //int leftValue = extractColumnsFromFunction(multiplication.getLeftExpression(), tuple);
                    //int rightValue = multiplicacionExpression(multiplication.getRightExpression(), tuple);
                    //return leftValue * rightValue;
                }
            }
        }
    }

    private void getMultiplicationParameters (Expression expression) {
        BinaryExpression multiplication = (BinaryExpression) expression;
        if (multiplication.getRightExpression() instanceof Column) {
            Column column = (Column) multiplication.getRightExpression();
            String columnNaeme = column.toString();
            tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.toString());
        }
        if (multiplication.getLeftExpression() instanceof Column) {
            Column column = (Column) multiplication.getLeftExpression();
            tableColumnsMentioned.computeIfAbsent(column.getTable().getName().toString(), k -> new HashSet<>()).add(column.toString());
        } else {
            getMultiplicationParameters(multiplication.getLeftExpression());
        }
    }


    private Expression getJoinCondition(String tableName) {
        for (Expression joinCondition : joinConditions) {

            BinaryExpression joinExpression = (BinaryExpression) joinCondition;
            Column leftExpression = (Column) joinExpression.getLeftExpression();
            Column righExpression = (Column) joinExpression.getRightExpression();

            if (leftExpression.getTable().getName().equals(tableName) || righExpression.getTable().getName().equals(tableName)) {
                //joinConditions.remove(joinCondition);
                return joinCondition;

            }
        }
        return null;
    }

    private boolean hasJoinCondition(String tableName) {
        for (Expression joinCondition : joinConditions) {

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

            // Identify Joins
            if (isJoinCondition((BinaryExpression) expression)) {
                //this.joinConditions.add(expression);
                this.joinConditions.add(0, expression);
                System.out.println("JOIN: " + expression);
            } else {

                this.selectionConditions.add(expression);

                //new implementation
                BinaryExpression binaryExpression = (BinaryExpression) expression;
                if (binaryExpression.getLeftExpression() instanceof Column) {
                    Column column = (Column) binaryExpression.getLeftExpression();
                    String columnName = column.getTable().getName();
                    selectionColumns.put(columnName, expression);
                } else {
                    // no columns - long values
                    selectionColumns.put("-1", expression);
                }
                System.out.println("WHERE: " + expression);
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
        }
        if (!this.joinConditions.isEmpty() || !this.joinElements.isEmpty()) {
            this.joinOperator = true;
        }
        if (!this.crossProductElements.isEmpty()) {
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
        if (!this.groupByElements.isEmpty()) {
            this.groupByOperator = true;
        }
    }


    /// ////////////////////////////////////
    /// ////////////////////////////////////
    /// ////////////////////////////////////



    public Operator buildQueryPlan(Statement statement) throws Exception {

        identifyElementsOperators(statement);
        identifyOperators();

        // For optimisation purposes, if query has projectionOperator true, it means that we could apply some projections before applying other operators
        if (projectionOperator) {
            identifyExpressions();
        }

        // Building a tree of operators. RootOperator is the variable that contains the whole tree.
        // Reset the root operator to start with the query plan
        rootOperator = null;

        // 1. Scan the first table that we have in the FROM clause
        // 2. If there is any selection condition for this table, apply it
        //rootOperator = scanRelation(fromTable);

        //rootOperator = scanRelation(queryTables.get(0));
        rootOperator = scanRelation(queryTables.keySet().iterator().next());

// 3. Apply the selections without columns. Ex: 1=1 or 2=1
//        if (selectionOperator) {
//            List<Expression> selectionsWithoutTable = getSelectionCondition("-1"); // -1 means no columns
//            for (Expression selectionCondition : selectionsWithoutTable) {
//                Operator selectOperator = new SelectOperator(rootOperator, selectionCondition);
//                rootOperator = selectOperator;
//            }
//        }

        // 4. After having all the selections applied, we can make the joins
        // Joins should do it in order based on Joins in the query. DONE
//        if (joinOperator && !joinConditions.isEmpty()) { // There is a join condition
//            for (Expression joinCondition : joinConditions) {
        if (joinOperator && !joinElements.isEmpty()) { // There is a join condition
            for (Expression joinCondition : joinElements) {
                BinaryExpression joinExpression = (BinaryExpression) joinCondition;

                // Identify the table on the root
//                Column leftColumn = (Column) joinExpression.getLeftExpression();
//                String leftTableName = leftColumn.getTable().toString();
//                Column rightcolumn = (Column) joinExpression.getRightExpression();
//                String rightTableName = rightcolumn.getTable().toString();
//
//                if (queryTables.get(rightTableName)) {
//                    rootOperator = new JoinOperator(rootOperator, scanRelation(leftTableName), joinCondition);
//                } else {
//                    rootOperator = new JoinOperator(rootOperator, scanRelation(rightTableName), joinCondition);
//                }


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
            Operator projectOperator = new ProjectOperator(rootOperator, projectionExpressions, null);
            rootOperator = projectOperator;
        }

        // Cross products without conditions
        if (joinOperator && !crossProductElements.isEmpty()) {
            for (Join join : crossProductElements) {
                // Identify the table on right side to scan it
                String rightTableName = join.toString();
                Operator scanRightTable = scanRelation(rightTableName);
                rootOperator = new JoinOperator(rootOperator, scanRightTable, null);
            }
        }

        // blocking Operato
        if (groupByOperator || sumOperator) {
            Operator groupByOperator = new SumOperator(rootOperator, groupByElements, sumExpressions, projectionExpressions);
            rootOperator = groupByOperator;
        }

        // Here we apply the last projection to keep the columns as the user requested and reduce the number of intermediate results
        // This project is the original one to leave the columns as the user requested



        // Last selection
//        if (selectionOperator) {
//            //List<Expression> selectionsWithoutTable = getSelectionCondition("-1"); // -1 means no columns
//            for (Expression selectionCondition : selectionConditions) {
//                Operator selectOperator = new SelectOperator(rootOperator, selectionCondition);
//                rootOperator = selectOperator;
//            }
//        }

        // Be carefull, keep distinct at the end of the operators
        if (distinctOperator) {
            Operator distinctOperator = new DuplicateEliminationOperator(rootOperator);
            rootOperator = distinctOperator;
        }


        // we can assume that order operator can be after projection.- It is good to delay sorting as late as possible, in
        //particular to do it after the projection(s), because there will be less data to sort that way.
        if (orderByOperator) {
            Operator orderByOperator = new SortOperator(rootOperator, orderByElements);
            rootOperator = orderByOperator;
        }

        return rootOperator;
    }


    // non-optional scan operator
    private Operator scanRelation(String fromTable) throws Exception {

        // 1. Scan Table
        Operator root = new ScanOperator(fromTable);

        // OPTIMISATION: Trivial query. Here we avoid to avoid operation that are not needed in the query, in case of 1 = 2
        if (selectionOperator) { // if there is a selection condition
            List<Expression> selectionsWithoutTable = getSelectionCondition("-1"); // -1 means no columns
            for (Expression selectionCondition : selectionsWithoutTable) {
                Operator selectOperator = new SelectOperator(root, selectionCondition);
                root = selectOperator;
            }
        }


// OPTIMISATION: if there is a selection condition for this table, apply it
        // Obviously it is most efficient to evaluate the selections as
        //early as possible
        if (selectionColumns.containsKey(fromTable)) {
            List<Expression> selectionsForTable = getSelectionCondition(fromTable);

            for (Expression selectionCondition : selectionsForTable) {
                Operator selectOperator = new SelectOperator(root, selectionCondition);
                root = selectOperator;
            }
        }

        // OPTIMISATION: if there are projections in the select we could apply including the columns that are needed for the subsequent operators. It is a way to reduce the intermediate results

//        if (projectionOperator) {
//            if (tableColumnsMentioned.containsKey(fromTable)) {
//
//                List<String> columns = new ArrayList<>(tableColumnsMentioned.get(fromTable));
//                Operator projectOperator = new ProjectOperator(root, null, columns);
//                root = projectOperator;
//            }
//        }



        // Modify the state of scan table to true
        queryTables.put(fromTable, true);
        return root;
    }

    private List<Expression> getSelectionCondition(String fromTable) {
        List<Expression> selectionsForTable = new ArrayList<>();
        for (Map.Entry<String, Expression> entry : selectionColumns.entrySet()) {
            if (entry.getKey().equals(fromTable)) {
                selectionsForTable.add(entry.getValue());
            }
        }
        return selectionsForTable;
    }


}