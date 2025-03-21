package ed.inf.adbs.blazedb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Database Catalog class that manages metadata about the database.
 * It maintains information about table file locations and schema definitions.
 * This class follows the Singleton pattern to ensure only one instance exists.
 */
public class Catalog {

    // Singleton instance
    private static Catalog instance;
    // Maps table names to their corresponding file paths
    private Map<String, String> fileMap;
    // Maps table names to their corresponding schema
    private Map<String, String[]> schemaMap;

    /**
     * Purpose: Private constructor to enforce Singleton pattern.
     * Initializes maps for file paths and schema storage.
     */
    private Catalog() {
        fileMap = new HashMap<>();
        schemaMap = new HashMap<>();
    }

    /**
     * Purpose: Get the singleton instance of the Catalog class. If no instance exists, a new one is created.
     * @return The Catalog instance
     */
    public static Catalog getInstance() {
        if (instance == null) { //If instance is null, create a new instance - singleton pattern
            instance = new Catalog();
        }
        return instance;
    }

    /**
     * Load the schema of the database from the schema.txt file
     * @param databaseDir The directory where the database is located
     * @throws Exception If there is an error while reading the schema file
     */
    public void loadSchema(String databaseDir) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(databaseDir + "/schema.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" "); // Split by space - it is described in the CW
                String tableName = parts[0]; // First element is the name
                String[] columns = new String[parts.length - 1];
                System.arraycopy(parts, 1, columns, 0, parts.length - 1); // Copy the rest of the elements
                schemaMap.put(tableName, columns);
                addTable(tableName, databaseDir + "/data" +"/" + tableName + ".csv"); // Since the path is in the file data and the file has the same name as the table
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error while reading schema file");
        }
    }

    /**
     * Add a table to the catalog with its corresponding file path
     * @param tableName The name of the table
     * @param fileTable The file path of the table
     */
    public void addTable(String tableName, String fileTable) {
        fileMap.put(tableName, fileTable);
    }

    /**
     * Get the file path of a table. This is used in the ScanOperator to read the table file.
     * @param tableName The name of the table
     * @return The file path of the table
     */
    public String getTableFilePath(String tableName) {
        return fileMap.get(tableName);
    }

    /**
     * Get the schema of a table. This is used to get the column names of a table.
     * @param tableName The name of the table
     * @return The schema of the table - An array containing the column names of the table
     */
    public String[] getSchema(String tableName) {
        return schemaMap.get(tableName);
    }

}
