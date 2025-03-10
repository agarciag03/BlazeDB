package ed.inf.adbs.blazedb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Database Catalog to keep track of information:
 *  1. Where a file for a given table is located
 *  2. What the schema of different tables is
 */
public class Catalog {

    //Using singleton pattern to ensure one instance
    private static Catalog instance;
    private Map<String, String> fileMap; //Map locations of tables to their names
    private Map<String, String[]> schemaMap; //Map schema of tables to their names

    private Catalog() {
        fileMap = new HashMap<>();
        schemaMap = new HashMap<>();
    }

    public static Catalog getInstance() {
        if (instance == null) { //If instance is null, create a new instance - singleton pattern
            instance = new Catalog();
        }
        return instance;
    }

    public void addTable(String tableName, String fileTable) {
        fileMap.put(tableName, fileTable);
    }

    public String getTableFilePath(String tableName) {
        return fileMap.get(tableName);
    }

    /**
     * Load the schema of the database from the schema.txt file
     * @param databaseDir The directory where the database is located
     * @throws Exception If there is an error while reading the schema file
     */
    public void loadSchema(String databaseDir) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(databaseDir + "/schema.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) { // Read line by line
                String[] parts = line.split(" "); // Split by space was given in the CW
                String tableName = parts[0];
                String[] columns = new String[parts.length - 1]; // because the first element is the name
                System.arraycopy(parts, 1, columns, 0, parts.length - 1); // Copy the rest of the elements
                schemaMap.put(tableName, columns);
                addTable(tableName, databaseDir + "/data" +"/" + tableName + ".csv");
            }
        } catch (IOException e) {
            e.printStackTrace(); // Print the stack trace
            throw new Exception("Error while reading schema file");
        }
    }

    public String[] getSchema(String tableName) {
        return schemaMap.get(tableName);
    }

}
