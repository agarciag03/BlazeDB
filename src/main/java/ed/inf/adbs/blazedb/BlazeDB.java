package ed.inf.adbs.blazedb;

/**
 * Lightweight in-memory database system.
 * Feel free to modify/move the provided functions. However, you must keep
 * the existing command-line interface, which consists of three arguments.
 *
 */

public class BlazeDB {

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0]; // Where database is
		String inputFile = args[1]; // Where the query input is
		String outputFile = args[2]; // The name of the file where the result will be written

		QueryInterpreter.interpretQuery(databaseDir, inputFile, outputFile);

	}

}
