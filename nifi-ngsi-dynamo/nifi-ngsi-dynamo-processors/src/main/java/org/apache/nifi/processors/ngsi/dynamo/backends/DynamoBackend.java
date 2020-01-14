package org.apache.nifi.processors.ngsi.dynamo.backends;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.ngsi.dynamo.utils.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DynamoBackend {
    private DynamoDB dynamoDB;
    private long id = new Date().getTime();

    public DynamoDB getDynamoDB() {
        return dynamoDB;
    }

    public void setDynamoDB(DynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
    }

    public CreateTableRequest createTableRequest(String tableName, String primaryKey) throws Exception {
        try {
            // Create the key schema for the given primary key
            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement()
                    .withAttributeName(primaryKey)
                    .withKeyType(KeyType.HASH));

            // Create the attribute definitions
            ArrayList<AttributeDefinition> attrDefs = new ArrayList<AttributeDefinition>();
            attrDefs.add(new AttributeDefinition()
                    .withAttributeName(primaryKey)
                    .withAttributeType("N"));

            // Create the table request given the table name, the key schema and the attribute definitios
            CreateTableRequest tableRequest = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attrDefs)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(5L)
                            .withWriteCapacityUnits(5L));

            return tableRequest;
        } catch (Exception e) {
            return null;
        } // try catch
    } // createTable

    public void createTable (String tableName, ComponentLog logger) throws Exception {
        // Create the table if not exists
        if (!isTableExist(tableName)) {
            CreateTableRequest createTableRequest = createTableRequest(tableName, NGSIConstants.DYNAMO_DB_PRIMARY_KEY);
            if (createTableRequest != null) {
                logger.debug("Creating DynamoDB table " + tableName);
                Table table = dynamoDB.createTable(createTableRequest);
                // Wait until the table is active
                logger.debug("Waiting until the DynamoDB table " + tableName + " becomes active");
                table.waitForActive();
            } else {
                logger.error("Error while creating the DynamoDB table request " + tableName);
            }

        } // end if
        else {
            logger.debug("The data will be added to an existing table: " + tableName);
        }
    }

    public boolean isTableExist(String tableName) {
        try {
            TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
            return true;
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException rnfe) {
            return false;
        }
    }

    public ArrayList<Item> getItemsToPut (Entity entity, long creationTime, String fiwareServicePath, String attrPersistence, ComponentLog logger ) {
        // Array of items to put data of each entity
        ArrayList<Item> itemsToPut = new ArrayList<Item>();

        long recvTimeTs = creationTime;
        String recvTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(creationTime);
        String entityId = entity.getEntityId();
        String entityType = entity.getEntityType();
        ArrayList<Attributes> attributes = entity.getEntityAttrs();

        switch(attrPersistence){
            case "row":
                for (Attributes attribute : attributes) {
                    String attrName = attribute.getAttrName();
                    String attrType = attribute.getAttrType();
                    String attrValue = attribute.getAttrValue();
                    String attrMetadata = "[]";

                    // El campo no puede estar vacío, si no DynamoDB da error. Por lo tanto si no hay campo metadatos o está vacio, pondremos [] en dicho campo.
                    if (attribute.getMetadataString() != null && attribute.getMetadataString() != "") {
                        attrMetadata = attribute.getMetadataString();
                    }

                    //One item for each attribute in a row
                    Item item = new Item()
                            .withPrimaryKey(NGSIConstants.DYNAMO_DB_PRIMARY_KEY, id)
                            .withDouble(NGSIConstants.RECV_TIME_TS, recvTimeTs)
                            .withString(NGSIConstants.RECV_TIME, recvTime)
                            .withString(NGSIConstants.FIWARE_SERVICE_PATH, fiwareServicePath)
                            .withString(NGSIConstants.ENTITY_ID, entityId)
                            .withString(NGSIConstants.ENTITY_TYPE, entityType)
                            .withString(NGSIConstants.ATTR_NAME, attrName)
                            .withString(NGSIConstants.ATTR_TYPE, attrType)
                            .withString(NGSIConstants.ATTR_VALUE, attrValue)
                            .withString(NGSIConstants.ATTR_MD, attrMetadata);
                    itemsToPut.add(item);
                    id++;
                } // end for
                break;

            case "column":
                // Only one item for all attributes. For each attribute two columns are added to the item: attrName, attrName_md
                Item item = new Item()
                        .withPrimaryKey(NGSIConstants.DYNAMO_DB_PRIMARY_KEY, id)
                        .withDouble(NGSIConstants.RECV_TIME_TS, recvTimeTs)
                        .withString(NGSIConstants.RECV_TIME, recvTime)
                        .withString(NGSIConstants.FIWARE_SERVICE_PATH, fiwareServicePath)
                        .withString(NGSIConstants.ENTITY_ID, entityId)
                        .withString(NGSIConstants.ENTITY_TYPE, entityType);
                for (Attributes attribute : attributes) {
                    String attrName = attribute.getAttrName();
                    String attrType = attribute.getAttrType();
                    String attrValue = attribute.getAttrValue();
                    String attrMetadata = "[]";

                    // El campo no puede estar vacío, si no DynamoDB da error. Por lo tanto si no hay campo metadatos o está vacio, pondremos [] en dicho campo.
                    if (attribute.getMetadataString() != null && attribute.getMetadataString() != "") {
                        attrMetadata = attribute.getMetadataString();
                    }
                    item.withString(attrName, attrValue)
                            .withString(attrName+"_md", attrMetadata);
                } //end for
                itemsToPut.add(item);
                id++;
                break;

            default:
                logger.error("Unknown attrPersistence" + attrPersistence+ " Please, use 'row' or 'column'");

        } //end switch

        return itemsToPut;
    }

    public void putItems (String tableName, Entity entity, long creationTime, String fiwareServicePath, String attrPersistence, ComponentLog logger) throws Exception{

        ArrayList<Item> itemsToPut = getItemsToPut(entity,creationTime,fiwareServicePath, attrPersistence, logger);
        TableWriteItems tableWriteItems = new TableWriteItems(tableName);
        tableWriteItems.withItemsToPut(itemsToPut);
        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(tableWriteItems);
    }


    public String buildTableName(String fiwareServicePath, Entity entity, String dataModel, boolean enableEncoding, boolean enableLowercase, ComponentLog logger) throws Exception {
        String tableName = "";
        String servicePath = (enableLowercase) ? fiwareServicePath.toLowerCase() : fiwareServicePath;
        String entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
        String entityType = (enableLowercase) ? entity.getEntityType().toLowerCase() : entity.getEntityType();

        if (enableEncoding) {
            switch (dataModel) {
                case "db-by-service-path":
                    tableName = NGSICharsets.encodeDynamoDB(servicePath);
                    break;
                case "db-by-entity":
                    tableName = NGSICharsets.encodeDynamoDB(servicePath)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodeDynamoDB(entityId)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodeDynamoDB(entityType);
                    break;
                default:
                    logger.error("Unknown data model '" + dataModel + "'. Please, use dm-by-service-path, dm-by-entity or dm-by-attribute");
                    break;
            } // switch
        } else {
            switch (dataModel) {
                case "db-by-service-path":
                    if ("/".equals(servicePath)) {
                        logger.error("Default service path '/' cannot be used with "
                                + "dm-by-service-path data model");
                    } // if

                    tableName = NGSICharsets.encode(servicePath, true, false);
                    break;
                case "db-by-entity":
                    String truncatedServicePath = NGSICharsets.encode(servicePath, true, false);

                    tableName = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_')
                            + NGSICharsets.encode(entityId, false, true) + "_"
                            + NGSICharsets.encode(entityType, false, true);
                    break;
                default:
                    System.out.println("Unknown data model '" + dataModel + "'. Please, use DMBYSERVICEPATH or  DMBYENTITY");
                    break;
            } // switch

        } // if else

        if (tableName.length() > NGSIConstants.DYNAMO_DB_MAX_NAME_LEN) {
            throw new Exception("Building table name '" + tableName
                    + "' and its length is greater than " + NGSIConstants.DYNAMO_DB_MAX_NAME_LEN);
        } // if

        else if (tableName.length() < NGSIConstants.DYNAMO_DB_MIN_NAME_LEN) {
            throw new Exception("Building table name '" + tableName
                    + "' and its length is lower than " + NGSIConstants.DYNAMO_DB_MIN_NAME_LEN);
        } // if

        return tableName;
    }

}




