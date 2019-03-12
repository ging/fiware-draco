package org.apache.nifi.processors.ngsi.ngsi.backends.postgresql;

import java.util.ArrayList;
import java.util.HashMap;

public class PostgreSQLCache {
    
    private HashMap<String, ArrayList<String>> cache = new HashMap<String, ArrayList<String>>();
    
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
        System.out.println("Checking if the schema (" + schemaName + ") exists");
        if (!cache.isEmpty()) {
            for (String schema : cache.keySet()) {
                if (schema.equals(schemaName)) {
                    System.out.println("Schema (" + schemaName + ") exists in Cache");
                    return true;
                } // if
            } // for

            System.out.println("Schema (" + schemaName + ") doesnt' exist in Cache");
            return false;
        } else {
            System.out.println("Cache is empty");
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

                    System.out.println("Checking if the table (" + tableName + ") belongs to (" + schemaName + ")");

                    if (tables.contains(tableName)) {
                        System.out.println("Table (" + tableName + ") was found in the specified schema ("
                                + schemaName + ")");
                        return true;
                    } else {
                        System.out.println("Table (" + tableName + ") wasn't found in the specified schema ("
                                + schemaName + ")");
                        return false;
                    } // if else
                } // if
            } // for
            
            System.out.println("Schema (" + schemaName + ") wasn't found in Cache");
            return false;
        } else {
            System.out.println("Cache is empty");
            return false;
        }
    } // isTableInCachedSchema
    
    /**
     * Adds a schema to the cache.
     * @param schemaName
     */
    public void persistSchemaInCache(String schemaName) {
        cache.put(schemaName, new ArrayList<String>());
        System.out.println("Schema (" + schemaName + ") added to cache");
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
                System.out.println("Table (" + tableName + ") added to schema (" + schemaName + ") in cache");
            } // if
        } // for
    } // persistTableInCache
    
} // PostgreSQLCache
