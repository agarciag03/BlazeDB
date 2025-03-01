package ed.inf.adbs.blazedb;

/**
 * Lightweight in-memory database system.
 *
 * Feel free to modify/move the provided functions. However, you must keep
 * the existing command-line interface, which consists of three arguments.
 *
 */

public class BlazeDB {

	public static void main(String[] args) throws Exception {

		/*
		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}
*/
		String databaseDir = args[0]; // Where database is
		String inputFile = args[1]; // Where the query input is
		String outputFile = args[2]; // The name of the file where the result will be written


//		String databaseDir = "samples/db"; // Where database is
//		String inputFile = "samples/input/example.sql"; // Where the query input is
//		String outputFile = "samples/output/example.csv";

		QueryInterpreter.interpretQuery(databaseDir, inputFile, outputFile);

		// REvision
//		Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Student WHERE Student.A = 1");
//
//		Select select = (Select) statement;
//		PlainSelect plainSelect1 = (PlainSelect) select.getSelectBody();




//		// Loading all table that we have in the schema
//		Catalog catalog = Catalog.getInstance(); // because of singleton pattern
//        catalog.loadSchema(databaseDir);

		// Just for demonstration, replace this function call with your logic
		//QueryInterpreter queryInterpreter = new QueryInterpreter();
		//QueryPlanBuilder queryPlanBuilder = new QueryPlanBuilder();
		// Build the query plan
		//Operator rootOperator = queryPlanBuilder.buildQueryPlan(queryInterpreter.parsingSQL(inputFile));
		// Execute the query plan
		//execute(rootOperator, outputFile);

		//parsingSQL(inputFile);
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement
	 * from a file or a string and prints the SELECT and WHERE clauses to screen.
	 */

/*
	public static void parsingSQL(String filename) {
		try {
			//Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
			Statement statement = CCJSqlParserUtil.parse("SELECT Student.*  FROM Student WHERE Student.A = 1");

			if (statement != null) {
				Select select = (Select) statement;
				System.out.println("Statement: " + select);
				System.out.println("SELECT items: " + select.getPlainSelect().getSelectItems());
				System.out.println("FROM clause: " + select.getPlainSelect().getFromItem());
				System.out.println("OTHER TABLES: " + select.getPlainSelect().getJoins());
				System.out.println("WHERE expression: " + select.getPlainSelect().getWhere());

				// Extracting the table name and the condition
				PlainSelect plainSelect = (PlainSelect) ((Select) statement).getSelectBody();
				String tableName = plainSelect.getFromItem().toString();
				Expression conditionExpression = plainSelect.getWhere();
				List<SelectItem> selectItems = (List<SelectItem>) (List<?>) plainSelect.getSelectItems();

				// Organising the tree of operators
				Operator scanOperator = new ScanOperator(tableName);
				Operator selectOperator = new SelectOperator(scanOperator, conditionExpression);
				Operator projectOperator = new ProjectOperator(selectOperator, selectItems);

				// Execute the query plan
				execute(projectOperator, "samples/output/output.txt");
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
	*/



	/**
	 * Executes the provided query plan by repeatedly calling `getNextTuple()`
	 * on the root object of the operator tree. Writes the result to `outputFile`.
	 *
	 * @param root The root operator of the operator tree (assumed to be non-null).
	 * @param outputFile The name of the file where the result will be written.
	 */
//	public static void execute(Operator root, String outputFile) throws Exception {
//		try {
//			// Create a BufferedWriter
//			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
//
//			// Iterate over the tuples produced by root
//			Tuple tuple = root.getNextTuple();
//			while (tuple != null) {
//				writer.write(tuple.toString());
//				writer.newLine();
//				tuple = root.getNextTuple();
//			}
//
//			// Close the writer
//			writer.close();
//		}
//		catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
}
