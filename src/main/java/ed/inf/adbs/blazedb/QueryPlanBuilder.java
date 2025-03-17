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

        Select select = (Select) statement; // Statement is always a Select
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        // 0. Identifying the table in the FROM clause
        //String fromTable = plainSelect.getFromItem().toString();
        queryTables.put(plainSelect.getFromItem().toString(), false);

        // 1. identifying projection (or AllColumns) and SUM Operator
        List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (selectItem.getExpression() instanceof AllColumns) {
                System.out.println("SELECT: AllCollumns");

            } else if (selectItem.getExpression() instanceof Function) {
                // sum is always instance as a Function

                Function function = (Function) selectItem.getExpression();
                if (function.getName().equalsIgnoreCase("SUM")) {

                    Expression sumExpression = function.getParameters();
                    //sumExpressions.add(sumExpression);
                    sumExpressions.add(function);//selectItem.getExpression());

                    System.out.println("SUM:  " + function.getParameters());
                }
            } else if (selectItem.getExpression() instanceof Column) {
                // selectItem could be a number or a column
                projectionExpressions.add(selectItem.getExpression());
                System.out.println("SELECT:  " + selectItem.getExpression());

            } else {
                System.out.println(" This select function is not allowed: " + selectItem);
            }
        }

        // 2. Identify Joins

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

                // identify Joins without Conditions
                if(!hasJoinCondition(join.toString())) {
                    System.out.println("JOIN - CROSS PRODUCT: " + join);
                    crossProductElements.add(join);
                } else {

                    System.out.println("organise the join conditions in left tree");
                }
            }
        }

        // Organise the join conditions in the left tree and in right order of conditions.

        //Implementation #1
//        if (joinsExpressions != null) {
//            for (String tableName : queryTables.keySet()) {
//                // identify Joins without Conditions
//                if (!joinConditions.isEmpty()) {
//                    joinElements.add(getJoinCondition(tableName));
//                    System.out.println("JOIN: " + joinElements);
//                }
//            }
//        }

        //Implementation #2
//        if (joinsExpressions != null) { // there is join expressions
//            for (String tableName : queryTables.keySet()) { // tables in order to left to right
//                // identify Joins without Conditions
//                if (!joinConditions.isEmpty()) { // there is join conditions which is the same than JoinExpressions - CHECK
//
//                    for (Expression joinCondition : joinConditions) {
//                        BinaryExpression joinExpression = (BinaryExpression) joinCondition;
//                        Column leftExpression = (Column) joinExpression.getLeftExpression();
//                        Column rightExpression = (Column) joinExpression.getRightExpression();
//
//                        String leftTable = leftExpression.getTable().getName();
//                        String rightTable = rightExpression.getTable().getName();
//
//                        // Verifica si una de las tablas es la actual en el bucle
//                        if (leftTable.equals(tableName) || rightTable.equals(tableName)) {
//
//                            // Revisa si el orden está incorrecto (rightTable debería estar después de leftTable)
//                            if (queryTables.containsKey(rightTable) && queryTables.containsKey(leftTable)
//                                    && getTableIndex(leftTable) > getTableIndex(rightTable)) {
//
//                                // Intercambia la expresión para que quede en el orden correcto
//                                BinaryExpression correctedJoin = new EqualsTo();
//                                correctedJoin.setLeftExpression(rightExpression);
//                                correctedJoin.setRightExpression(leftExpression);
//
//                                // Reemplaza la expresión original por la corregida
//                                joinConditions.remove(joinCondition);
//                                joinElements.add(correctedJoin);
//                            }
//                        }
//                    }
//
//                    //joinElements.add(getJoinCondition(tableName));
//                    System.out.println("JOIN: " + joinElements);
//                }
//            }
//        }

        //Implementation #3
//        if (joinsExpressions != null) { // there is join expressions
//            for (String tableName : queryTables.keySet()) { // tables in order to left to right
//
//
//                // identify Joins without Conditions
//                if (!joinConditions.isEmpty()) {
//                    Iterator<Expression> iterator = joinConditions.iterator();
//                    while (iterator.hasNext()) {
//                        Expression joinCondition = iterator.next();
//                        BinaryExpression joinExpression = (BinaryExpression) joinCondition;
//                        Column leftExpression = (Column) joinExpression.getLeftExpression();
//                        Column rightExpression = (Column) joinExpression.getRightExpression();
//
//                        String leftTable = leftExpression.getTable().getName();
//                        String rightTable = rightExpression.getTable().getName();
//
//                        // Verifica si una de las tablas es la actual en el bucle
//                        if (leftTable.equals(tableName) || rightTable.equals(tableName)) {
//
//                            // Verifica si el orden está incorrecto
//                            if (queryTables.containsKey(rightTable) && queryTables.containsKey(leftTable)
//                                    && getTableIndex(leftTable) > getTableIndex(rightTable)) {
//
//                                // Crea la nueva expresión corregida
//                                // BE CAREFULL CHECK WITH lowerthan, or greaterthan
//
//                                BinaryExpression correctedJoin = new EqualsTo();
//                                correctedJoin.setLeftExpression(rightExpression);
//                                correctedJoin.setRightExpression(leftExpression);
//
//                                // Elimina el elemento actual de la lista de forma segura
//                                joinConditions.remove(joinCondition);
//
//                                // Agrega la nueva condición corregida
//                                joinElements.add(correctedJoin);
//
//
//                            } else { // si el orden es correcto
//                                joinConditions.remove(joinCondition);
//                                joinElements.add(joinCondition);
//                            }
//                        }
//                    }
//                }
//            }
//        }

        //Implementation #4
//        if (joinsExpressions != null) { // Hay expresiones de JOIN
//            for (String tableName : queryTables.keySet()) { // Tablas en orden de izquierda a derecha
//
//                boolean modified;
//                do {
//                    modified = false; // Indicador de cambios en la lista
//                    Iterator<Expression> iterator = joinConditions.iterator();
//
//                    while (iterator.hasNext()) {
//                        Expression joinCondition = iterator.next();
//                        BinaryExpression joinExpression = (BinaryExpression) joinCondition;
//                        Column leftExpression = (Column) joinExpression.getLeftExpression();
//                        Column rightExpression = (Column) joinExpression.getRightExpression();
//
//                        String leftTable = leftExpression.getTable().getName();
//                        String rightTable = rightExpression.getTable().getName();
//
//                        // Verifica si la condición de JOIN involucra la tabla actual
//                        if (leftTable.equals(tableName) || rightTable.equals(tableName)) {
//
//                            // Verifica si el orden está incorrecto
//                            if (queryTables.containsKey(rightTable) && queryTables.containsKey(leftTable)
//                                    && getTableIndex(leftTable) > getTableIndex(rightTable)) {
//
//                                // Corrige la expresión de JOIN
//                                BinaryExpression correctedJoin = new EqualsTo();
//                                correctedJoin.setLeftExpression(rightExpression);
//                                correctedJoin.setRightExpression(leftExpression);
//
//                                iterator.remove();  // ✅ Elimina de manera segura
//                                joinElements.add(correctedJoin);
//                                modified = true; // Hubo un cambio, repetir el bucle
//
//                            } else { // Si el orden es correcto
//                                iterator.remove();  // ✅ Elimina de manera segura
//                                joinElements.add(joinCondition);
//                                modified = true; // Hubo un cambio, repetir el bucle
//                            }
//                        }
//                    }
//                } while (modified); // Si se hizo un cambio, vuelve a recorrer la lista
//            }
//        }

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
                            if (joinExpression instanceof EqualsTo) {
                                correctedJoin = new EqualsTo();
                            } else if (joinExpression instanceof NotEqualsTo) {
                                correctedJoin = new NotEqualsTo();
                            } else if (joinExpression instanceof GreaterThan) {
                                correctedJoin = new MinorThan(); // `>` se convierte en `<`
                            } else if (joinExpression instanceof GreaterThanEquals) {
                                correctedJoin = new MinorThanEquals(); // `>=` se convierte en `<=`
                            } else if (joinExpression instanceof MinorThan) {
                                correctedJoin = new GreaterThan(); // `<` se convierte en `>`
                            } else { // MinorThanEquals
                                correctedJoin = new GreaterThanEquals(); // `<=` se convierte en `>=`
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

        // Identify all the expression - columns needed for each table
        identifyExpressions();
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
        List<Expression> expressions = new ArrayList<>();
        expressions.addAll(projectionExpressions);
        expressions.addAll(selectionConditions);
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

    private void checkOperators() {

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

    public Operator buildQueryPlan(Statement statement) throws Exception {
        identifyElementsOperators(statement);
        checkOperators();

        rootOperator = null; // Reset the root operator

        // 1. Scan the first table that we have in the FROM clause
        // 2. If there is any selection condition for this table, apply it
        //rootOperator = scanRelation(fromTable);

        //rootOperator = scanRelation(queryTables.get(0));
        rootOperator = scanRelation(queryTables.keySet().iterator().next());

// 3. Apply the selections without columns. Ex: 1=1 or 2=1
        if (selectionOperator) {
            List<Expression> selectionsWithoutTable = getSelectionCondition("-1"); // -1 means no columns
            for (Expression selectionCondition : selectionsWithoutTable) {
                Operator selectOperator = new SelectOperator(rootOperator, selectionCondition);
                rootOperator = selectOperator;
            }
        }

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

        // blocking
        if (groupByOperator || sumOperator) {
            Operator groupByOperator = new SumOperator(rootOperator, groupByElements, sumExpressions, projectionExpressions);
            rootOperator = groupByOperator;
        }

        // we can assume that order operator can be after projection.
        if (orderByOperator) {
            Operator orderByOperator = new SortOperator(rootOperator, orderByElements);
            rootOperator = orderByOperator;
        }

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

        return rootOperator;
    }

    private Operator scanRelation(String fromTable) throws Exception {

        // 1. Scan Table
        Operator root = new ScanOperator(fromTable);

// OPTIMISATION: if there is a selection condition for this table, apply it
        if (selectionColumns.containsKey(fromTable)) {
            List<Expression> selectionsForTable = getSelectionCondition(fromTable);

            for (Expression selectionCondition : selectionsForTable) {
                Operator selectOperator = new SelectOperator(root, selectionCondition);
                root = selectOperator;
            }
        }

        // OPTIMISATION: if there is a selection for this table, apply it
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