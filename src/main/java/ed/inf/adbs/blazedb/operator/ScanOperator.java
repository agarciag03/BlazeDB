package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Operator to scan the table file and return the tuples with values and column names.
 */
public class ScanOperator extends Operator{
    private String tableName;
    private String[] columnNames;
    private String filePath;
    private BufferedReader reader;

    /**
     * Constructor to initialize the scan operator.
     * @param tableName Name of the table to scan
     * @throws Exception If the table file is not found
     */
    public ScanOperator(String tableName) throws Exception {
        try {
            this.tableName = tableName;
            this.columnNames = Catalog.getInstance().getSchema(tableName);
            this.filePath = Catalog.getInstance().getTableFilePath(tableName);
            reset();

            } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Table file not found");
        }
    }

    /**
     * Get the next tuple from the file. This method read the file line by line and create a tuple
     * object with values and column names
     * @return Tuple object with values and column names
     * @throws Exception If the file is not found
     */
    @Override
    public Tuple getNextTuple() throws Exception {
        Tuple tuple = null;
        String line = reader.readLine();
        if (line != null){
            tuple = new Tuple(line, tableName, columnNames);
        }
        return tuple;
    }

    /**
     * Reset the file reader to read the file from the beginning
     * @throws Exception If the file is not found
     */
    @Override
    public void reset() throws Exception {
        if (reader != null) {
            reader.close();
        }
        reader = new BufferedReader(new FileReader(filePath)); // Open the file again
    }
}
