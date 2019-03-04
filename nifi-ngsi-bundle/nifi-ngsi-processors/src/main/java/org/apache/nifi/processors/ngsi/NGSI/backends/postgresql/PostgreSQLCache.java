package org.apache.nifi.processors.ngsi.NGSI.backends.postgresql;

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.nifi.processors.ngsi.log.DracoLogger;

public class PostgreSQLCache {
    
    private HashMap<String, ArrayList<String>> cache = new HashMap<String, ArrayList<String>>();
    private static final DracoLogger LOGGER = new DracoLogger(PostgreSQLCache.class);
    
    public HashMap<String, ArrayList<String>> getCache() {
        return cache;
    } // getCache
    
    public void setCache(HashMap<String, ArrayList<String>> cache) {
        this.cache = cache;
    } // setCache
    
    /**
     * Gets if the schema is in the cache.
     * @param schemaName
     * @return True if the schema is in the cache, otherwise false
     */
    public boolean isSchemaInCache(String schemaName) {
        LOGGER.debug("Checking if the schema (" + schemaName + ") exists");
        if (!cache.isEmpty()) {
            for (String schema : cache.keySet()) {
                if (schema.equals(schemaName)) {
                    LOGGER.debug("Schema (" + schemaName + ") exists in Cache");
                    return true;
                } // if
            } // for

            LOGGER.debug("Schema (" + schemaName + ") doesnt' exist in Cache");
            return false;
        } else {
            LOGGER.debug("Cache is empty");
            return false;
        } // if else

    } // isSchemaInCache
    
    /**
     * Gets if a table within a shema is in the cache.
     * @param schemaName
     * @param tableName
     * @return True if the table within the shema is in the cache, otherwise false
     */
    public boolean isTableInCachedSchema(String schemaName, String tableName) {
        if (!cache.isEmpty()) {
            
            for (String schema : cache.keySet()) {
                if (schema.equals(schemaName)) {
                    ArrayList<String> tables = cache.get(schemaName);

                    LOGGER.info("Checking if the table (" + tableName + ") belongs to (" + schemaName + ")");

                    if (tables.contains(tableName)) {
                        LOGGER.debug("Table (" + tableName + ") was found in the specified schema ("
                                + schemaName + ")");
                        return true;
                    } else {
                        LOGGER.debug("Table (" + tableName + ") wasn't found in the specified schema ("
                                + schemaName + ")");
                        return false;
                    } // if else
                } // if
            } // for
            
            LOGGER.debug("Schema (" + schemaName + ") wasn't found in Cache");
            return false;
        } else {
            LOGGER.debug("Cache is empty");
            return false;
        }
    } // isTableInCachedSchema
    
    /**
     * Adds a schema to the cache.
     * @param schemaName
     */
    public void persistSchemaInCache(String schemaName) {
        cache.put(schemaName, new ArrayList<String>());
        LOGGER.debug("Schema (" + schemaName + ") added to cache");
    } // persistSchemaInCache
    
    /**
     * Adds a table within a schema to the cache.
     * @param schemaName
     * @param tableName
     */
    public void persistTableInCache(String schemaName, String tableName) {
        for (String schema : cache.keySet()) {
            if (schema.equals(schemaName)) {
                ArrayList<String> tableNames = cache.get(schemaName);
                tableNames.add(tableName);
                cache.put(schemaName, tableNames);
                LOGGER.debug("Table (" + tableName + ") added to schema (" + schemaName + ") in cache");
            } // if
        } // for
    } // persistTableInCache
    
} // PostgreSQLCache
