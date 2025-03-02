package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.Operator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * The QueryInterpreter class is responsible for interpreting the SQL query reading the statement from the query file
 */

public class QueryInterpreter {

    public static void interpretQuery(String databaseDir, String inputFile, String outputFile) throws Exception {
        // Loading all table that we have in the schema
        Catalog catalog = Catalog.getInstance(); // because of singleton pattern
        catalog.loadSchema(databaseDir);

        Statement select = parsingSQL(inputFile);
        Operator rootOperator = QueryPlanBuilder.buildQueryPlan(select);
        execute(rootOperator, outputFile);

    }

    // Method to parse the SQL query - Catch any problem at parsing time - Review Exceptions
    /**
     * Example method for getting started with JSQLParser. Reads SQL statement
     * from a file or a string and prints the SELECT and WHERE clauses to screen.
     */
    private static Statement parsingSQL(String inputFile) throws Exception {
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
            System.out.println(inputFile + ": " + statement);
            //Statement statement = CCJSqlParserUtil.parse("SELECT Student.A  FROM Student WHERE Student.A = 1");
//            PlainSelect plainSelect = null;
//
//            if (statement != null) {
//                Select select = (Select) statement;
//                plainSelect = (PlainSelect) select.getSelectBody();
//
//                System.out.println("Statement: " + select);
////                System.out.println("SELECT items: " + select.getPlainSelect().getSelectItems());
////                System.out.println("FROM clause: " + select.getPlainSelect().getFromItem());
////                System.out.println("OTHER TABLES: " + select.getPlainSelect().getJoins());
////                System.out.println("WHERE expression: " + select.getPlainSelect().getWhere());
//
//            }

            return statement;

        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Executes the provided query plan by repeatedly calling `getNextTuple()`
     * on the root object of the operator tree. Writes the result to `outputFile`.
     *
     * @param root The root operator of the operator tree (assumed to be non-null).
     * @param outputFile The name of the file where the result will be written.
     */
    public static void execute(Operator root, String outputFile) throws Exception {
        try {
            // Create a BufferedWriter
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            // Iterate over the tuples produced by root
            Tuple tuple = root.getNextTuple();
            while (tuple != null) {
                writer.write(tuple.toString());
                writer.newLine();
                tuple = root.getNextTuple();
            }

            // Close the writer
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
