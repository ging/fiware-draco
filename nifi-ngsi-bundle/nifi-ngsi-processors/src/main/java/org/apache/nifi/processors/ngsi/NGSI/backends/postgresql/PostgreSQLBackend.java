package org.apache.nifi.processors.ngsi.NGSI.backends.postgresql;

public interface PostgreSQLBackend {

    /**
     * Creates a schema, given its name, if not exists.
     * @param schemaName
     * @throws Exception
     */
    void createSchema(String schemaName) throws Exception;

    /**
     * Creates a table, given its name, if not exists in the given schema.
     * @param schemaName
     * @param tableName
     * @param fieldNames
     * @throws Exception
     */
    void createTable(String schemaName, String tableName, String fieldNames) throws Exception;

    /**
     * Insert already processed context data into the given table within the given database.
     * @param schemaName
     * @param tableName
     * @param fieldNames
     * @param fieldValues
     * @throws Exception
     */
    void insertContextData(String schemaName, String tableName, String fieldNames, String fieldValues) throws Exception;

} // PostgreSQLBackend
