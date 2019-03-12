package org.apache.nifi.processors.ngsi.ngsi.aggregators;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import org.apache.nifi.processors.ngsi.ngsi.backends.postgresql.PostgreSQLBackendImpl;
import org.apache.nifi.processors.ngsi.ngsi.utils.*;

public abstract class PostgreSQLAggregator {
  
    // string containing the data fieldValues
    protected String aggregation;

    protected String service;
    protected String servicePath;
    protected String destination;
    protected String entityForNaming;
    protected String schemaName;
    protected String tableName;
    protected String typedFieldNames;
    protected String fieldNames;

    public PostgreSQLAggregator() {
        aggregation = "";
    } // PostgreSQLAggregator

    public String getAggregation() {
        return aggregation;
    } // getAggregation

    public String getSchemaName(boolean enableLowercase) {
        if (enableLowercase) {
            return schemaName.toLowerCase();
        } else {
            return schemaName;
        } // if else
    } // getDbName

    public String getTableName(boolean enableLowercase) {
        if (enableLowercase) {
            return tableName.toLowerCase();
        } else {
            return tableName;
        } // if else
    } // getTableName
    
    public String getTypedFieldNames() {
        return typedFieldNames;
    } // getTypedFieldNames

    public String getFieldNames() {
        return fieldNames;
    } // getFieldNames

    public void initialize(String fiwareService,String fiwareServicePath, Entity entity,String dataModel, boolean enableEncoding) throws Exception {
        service = fiwareService;
        servicePath = fiwareServicePath;
        destination = entity.getEntityId();
        entityForNaming = entity.getEntityType();
        schemaName = buildSchemaName(service, enableEncoding);
        tableName = buildTableName(servicePath, entity, dataModel, enableEncoding);
    } // initialize

    public abstract void aggregate(long creationTime,Entity entity,String servicePath);

  // PostgreSQLAggregator

/**
 * Class for aggregating batches in row mode.
 */
private class RowAggregator extends PostgreSQLAggregator {

    @Override
    public void initialize(String fiwareService, String fiwareServicePath, Entity entity, String dataModel, boolean enableEncoding) throws Exception {
        super.initialize(fiwareService, fiwareServicePath, entity, dataModel, enableEncoding);
        typedFieldNames = "("
                + NGSIConstants.RECV_TIME_TS + " bigint,"
                + NGSIConstants.RECV_TIME + " text,"
                + NGSIConstants.FIWARE_SERVICE_PATH + " text,"
                + NGSIConstants.ENTITY_ID + " text,"
                + NGSIConstants.ENTITY_TYPE + " text,"
                + NGSIConstants.ATTR_NAME + " text,"
                + NGSIConstants.ATTR_TYPE + " text,"
                + NGSIConstants.ATTR_VALUE + " text,"
                + NGSIConstants.ATTR_MD + " text"
                + ")";
        fieldNames = "("
                + NGSIConstants.RECV_TIME_TS + ","
                + NGSIConstants.RECV_TIME + ","
                + NGSIConstants.FIWARE_SERVICE_PATH + ","
                + NGSIConstants.ENTITY_ID + ","
                + NGSIConstants.ENTITY_TYPE + ","
                + NGSIConstants.ATTR_NAME + ","
                + NGSIConstants.ATTR_TYPE + ","
                + NGSIConstants.ATTR_VALUE + ","
                + NGSIConstants.ATTR_MD
                + ")";
    } // initialize

    @Override
    public void aggregate(long creationTime, Entity entity, String servicePath){
        // get the event headers
        long recvTimeTs = creationTime;
        String recvTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(recvTimeTs);

        // get the event body
        String entityId = entity.getEntityId();
        String entityType = entity.getEntityType();
        System.out.println("Processing context element (id=" + entityId + ", type="
                    + entityType + ")");

        // iterate on all this context element attributes, if there are attributes
        ArrayList<Attributes> attributes = entity.getEntityAttrs();

        if (attributes == null || attributes.isEmpty()) {
            System.out.println("No attributes within the notified entity, nothing is done (id=" + entityId
                        + ", type=" + entityType + ")");
            return;
        } // if

        for (Attributes attribute : attributes) {
            String attrName = attribute.getAttrName();
            String attrType = attribute.getAttrType();
            String attrValue = attribute.getAttrValue();
            String attrMetadata = attribute.getMetadataString();
            System.out.println("Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");
            // create a line and aggregate it
            String row = "('"
                    + recvTimeTs + "','"
                    + recvTime + "','"
                    + servicePath + "','"
                    + entityId + "','"
                    + entityType + "','"
                    + attrName + "','"
                    + attrType + "','"
                    + attrValue + "',";
            if (!attrMetadata.isEmpty()){
              row += "'" + attrMetadata + "'";
            }
            else{
              row += null;
            }
            if (aggregation.isEmpty()) {
                aggregation += row + ")";
            } else {
                aggregation += "," + row + ")";
            } // if else
        } // for
    } // aggregate

} // RowAggregator

/**
 * Class for aggregating batches in column mode.
 */
private class ColumnAggregator extends PostgreSQLAggregator {

    @Override
    public void initialize(String fiwareService,String fiwareServicePath, Entity entity, String dataModel, boolean enableEncoding) throws Exception {
        super.initialize(fiwareService, fiwareServicePath, entity, dataModel, enableEncoding);

        // particulat initialization
        typedFieldNames = "(" + NGSIConstants.RECV_TIME + " text,"
                + NGSIConstants.FIWARE_SERVICE_PATH + " text,"
                + NGSIConstants.ENTITY_ID + " text,"
                + NGSIConstants.ENTITY_TYPE + " text";
        fieldNames = "(" + NGSIConstants.RECV_TIME + ","
                + NGSIConstants.FIWARE_SERVICE_PATH + ","
                + NGSIConstants.ENTITY_ID + ","
                + NGSIConstants.ENTITY_TYPE;

        // iterate on all this context element attributes, if there are attributes
        ArrayList<Attributes> attributes = entity.getEntityAttrs();

        if (attributes == null || attributes.isEmpty()) {
            return;
        } // if

        for (Attributes attribute : attributes) {
            String attrName = attribute.getAttrName();
            typedFieldNames += "," + attrName + " text," + attrName + "_md text";
            fieldNames += "," + attrName + "," + attrName + "_md";
        } // for

        typedFieldNames += ")";
        fieldNames += ")";
    } // initialize

    @Override
    public void aggregate(long creationTime,Entity entity,String servicePath) {
        // get the event headers
        long recvTimeTs = creationTime;
        String recvTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(recvTimeTs);

        // get the getRecvTimeTs body
        String entityId =  entity.getEntityId();
        String entityType = entity.getEntityType();
        System.out.println("Processing context element (id=" + entityId + ", type="
                    + entityType + ")");

        // iterate on all this context element attributes, if there are attributes
        ArrayList<Attributes> attributes = entity.getEntityAttrs();

        if (attributes == null || attributes.isEmpty()) {
            System.out.println("No attributes within the notified entity, nothing is done (id=" + entityId
                        + ", type=" + entityType + ")");
            return;
        } // if

        String column = "('" + recvTime + "','" + servicePath + "','" + entityId + "','" + entityType + "'";
                    
        for (Attributes attribute : attributes) {
            String attrName = attribute.getAttrName();
            String attrType = attribute.getAttrType();
            String attrValue = attribute.getAttrValue();
            String attrMetadata = attribute.getMetadataString();
            System.out.println("Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");

            // create part of the column with the current attribute (a.k.a. a column)
            column += ",'" + attrValue + "',";
            if (!attrMetadata.isEmpty()){
              column += "'" + attrMetadata + "'";
            }
            else{
              column += null;
            }
        } // for

        // now, aggregate the column
        if (aggregation.isEmpty()) {
            aggregation = column + ")";
        } else {
            aggregation += "," + column + ")";
        } // if else
    } // aggregate

} // ColumnAggregator

public PostgreSQLAggregator getAggregator(String rowAttrPersistence) {
    if ("row".equals(rowAttrPersistence)) {
        return new RowAggregator();
    } else if ("column".equals(rowAttrPersistence)){
        return new ColumnAggregator();
    } // if else
    return null;
} // getAggregator

public void persistAggregation(PostgreSQLAggregator aggregator, boolean enableLowercase, PostgreSQLBackendImpl persistenceBackend) throws Exception {
    //String aggregation = aggregator.getAggregation();
    String typedFieldNames = aggregator.getTypedFieldNames();
    String fieldNames = aggregator.getFieldNames();
    String fieldValues = aggregator.getAggregation();
    String schemaName = aggregator.getSchemaName(enableLowercase);
    String tableName = aggregator.getTableName(enableLowercase);

    System.out.println("Persisting data at NGSIPostgreSQLSink. Schema ("
                + schemaName + "), Table (" + tableName + "), Fields (" + fieldNames + "), Values ("
                + fieldValues + ")");
     
    try {
      //if (aggregator instanceof RowAggregator) {
          persistenceBackend.createSchema(schemaName);
          persistenceBackend.createTable(schemaName, tableName, typedFieldNames);
      //} // if
        // creating the database and the table has only sense if working in row mode, in column node
        // everything must be provisioned in advance
      persistenceBackend.insertContextData(schemaName, tableName, fieldNames, fieldValues);
    } catch (Exception e) {
      throw new Exception("-, " + e.getMessage());
    } // try catch
} // persistAggregation

/**
 * Creates a PostgreSQL DB name given the FIWARE service.
 * @param service
 * @return The PostgreSQL DB name
 * @throws DracoBadConfiguration
 */
public String buildSchemaName(String service, boolean enableEncoding) throws Exception {
    String name;
        
    if (enableEncoding) {
        name = NGSICharsets.encodePostgreSQL(service);
    } else {
        name = NGSICharsets.encode(service, false, true);
    } // if else

    if (name.length() > NGSIConstants.MYSQL_MAX_NAME_LEN) {
        throw new Exception("Building schema name '" + name
                + "' and its length is greater than " + NGSIConstants.MYSQL_MAX_NAME_LEN);
    } // if

    return name;
} // buildSchemaName

/**
 * Creates a PostgreSQL table name given the FIWARE service path, the entity and the attribute.
 * @param servicePath
 * @param entity
 * @param attribute
 * @return The PostgreSQL table name
 * @throws DracoBadConfiguration
 */
 public String buildTableName(String servicePath, Entity entity, String dataModel, boolean enableEncoding) throws Exception {
    String name;
    String entityId = entity.getEntityId();
    String entityType = entity.getEntityType();

    if (enableEncoding) {
        switch(dataModel) {
            case "db-by-service-path":
                name = NGSICharsets.encodePostgreSQL(servicePath);
                break;
            case "db-by-entity":
                name = NGSICharsets.encodePostgreSQL(servicePath)
                         + CommonConstants.CONCATENATOR
                         + NGSICharsets.encodePostgreSQL(entityId)
                         + CommonConstants.CONCATENATOR
                         + NGSICharsets.encodePostgreSQL(entityType);
                break;
            default:
                throw new Exception("Unknown data model '" + dataModel.toString()
                        + "'. Please, use dm-by-service-path or dm-by-entity");
        } // switch
    } else {
        switch(dataModel) {
            case "db-by-service-path":
                if ("/".equals(servicePath)) {
                    throw new DracoBadConfiguration("Default service path '/' cannot be used with "
                            + "dm-by-service-path data model");
                } // if
                   
                name = NGSICharsets.encode(servicePath, true, false);
                break;
            case "db-by-entity":
                String truncatedServicePath = NGSICharsets.encode(servicePath, true, false);
                name = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_')
                                + NGSICharsets.encode(entityId, false, true) + "_"
                                + NGSICharsets.encode(entityType, false, true);
                break;
            default:
                throw new DracoBadConfiguration("Unknown data model '" + dataModel
                            + "'. Please, use DMBYSERVICEPATH or DMBYENTITY");
        } // switch
    } // if else

    if (name.length() > NGSIConstants.MYSQL_MAX_NAME_LEN) {
        throw new Exception("Building table name '" + name
                + "' and its length is greater than " + NGSIConstants.MYSQL_MAX_NAME_LEN);
    } // if

    return name;
  } // buildTableName
}
