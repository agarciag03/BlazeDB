package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.Operator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * The QueryInterpreter class is responsible for interpreting and executing SQL queries.
 * It reads SQL statements from an input file, processes them into an operator tree,
 * and writes the query results to an output file.
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

        // Loading all tables from the schema to the catalog
        Catalog catalog = Catalog.getInstance();
        catalog.loadSchema(databaseDir);

        // Reading the SQL statement from the input file
        Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
        System.out.println(inputFile + ": " + statement);

        // Building the query plan
        QueryPlanBuilder queryPlanBuilder = new QueryPlanBuilder();
        Operator treeOperator = queryPlanBuilder.buildQueryPlan(statement);

        // Executing the query plan:
        execute(treeOperator, outputFile);

    }



    /**
     * Executes the provided query plan by repeatedly calling `getNextTuple()`
     * on the root object of the operator tree. Writes the result to `outputFile`.
     *
     * @param root The root operator of the operator tree. If the root is null it is because the query is trivial(OPTIMISATION)
     * @param outputFile The name of the file where the result will be written.
     * @throws Exception If an error occurs while writing to the output file.
     */
    public static void execute(Operator root, String outputFile) throws Exception {
        try {

            // Create a BufferedWriter
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            if (root != null) {
                // Iterate over the tuples produced by root operator and write them to the output file
                Tuple tuple = root.getNextTuple();
                while (tuple != null) {
                    writer.write(tuple.toString());
                    writer.newLine();
                    tuple = root.getNextTuple();
                }
            } else {
                // OPTIMISATION: The root will be null if an always-false (trivial) condition is detected during query planning.
                // Therefore, an empty string will be written to the output file without need to compute the query plan.
                writer.write("");
            }

            // Close the writer
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}