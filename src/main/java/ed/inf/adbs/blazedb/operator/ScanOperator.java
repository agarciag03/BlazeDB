package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;

import java.io.BufferedReader;
import java.io.FileReader;

public class ScanOperator extends Operator{
    private String tableName;
    private String[] columnNames;
    private String filePath;
    private BufferedReader reader;


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

    @Override
    public Tuple getNextTuple() throws Exception {
        Tuple tuple = null;
        String line = reader.readLine();
        if (line != null){
            tuple = new Tuple(line, tableName, columnNames);
        }
        return tuple;
    }

    @Override
    public void reset() throws Exception {
        if (reader != null) {
            reader.close();
        }
        reader = new BufferedReader(new FileReader(filePath)); // Open the file again
    }
}
