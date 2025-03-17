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

    /**
     * Interprets the SQL query provided in the input file and writes the result to the output file.
     *
     * @param databaseDir The directory where the database files are stored.
     * @param inputFile The name of the file containing the SQL query.
     * @param outputFile The name of the file where the result will be written.
     */
    public static void interpretQuery(String databaseDir, String inputFile, String outputFile) throws Exception {

        // Loading all table that we have in the schema
        Catalog catalog = Catalog.getInstance();
        catalog.loadSchema(databaseDir);

        Statement statement = parsingSQL(inputFile);
        QueryPlanBuilder queryPlanBuilder = new QueryPlanBuilder();
        Operator rootOperator = queryPlanBuilder.buildQueryPlan(statement);
        long startTime = System.nanoTime();
        execute(rootOperator, outputFile);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;
        System.out.println("La consulta tomó " + duration + " milisegundos para ejecutarse.");

    }

    /**
     * Reads SQL statement from query file and parses it using the CCJSqlParserUtil.
     * This method catches any exceptions that occur during parsing and prints an error message.
     */
    private static Statement parsingSQL(String inputFile) throws Exception {
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
            System.out.println(inputFile + ": " + statement);

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
