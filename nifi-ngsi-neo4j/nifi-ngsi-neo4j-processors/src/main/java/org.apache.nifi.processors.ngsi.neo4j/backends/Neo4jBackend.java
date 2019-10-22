package org.apache.nifi.processors.ngsi.neo4j.backends;

import org.apache.nifi.processors.ngsi.ngsi.utils.CommonConstants;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSICharsets;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIConstants;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;

public class Neo4jBackend {

    public ArrayList listOfFields (String attrPersistence){
        ArrayList<String> aggregation = new ArrayList<>();
        if (attrPersistence.compareToIgnoreCase("row")==0){
            aggregation.add(NGSIConstants.RECV_TIME_TS);
            aggregation.add(NGSIConstants.RECV_TIME);
            aggregation.add(NGSIConstants.FIWARE_SERVICE_PATH);
            aggregation.add(NGSIConstants.ENTITY_ID);
            aggregation.add(NGSIConstants.ENTITY_TYPE);
            aggregation.add(NGSIConstants.ATTR_NAME);
            aggregation.add(NGSIConstants.ATTR_TYPE);
            aggregation.add(NGSIConstants.ATTR_VALUE);
            aggregation.add(NGSIConstants.ATTR_MD);
        }else if(attrPersistence.compareToIgnoreCase("column")==0){
            //TBD
            System.out.println("column");
        }
        return aggregation;
    }
    public String getValuesForInsert(Entity entity, long creationTime, String fiwareServicePath) {
        String valuesForInsert = "";

        for (int i = 0; i < entity.getEntityAttrs().size(); i++) {
            if (i == 0) {
                valuesForInsert += "(now(),";
            } else {
                valuesForInsert += ",(";
            } // if else

            valuesForInsert += "'" + creationTime + "'";
            valuesForInsert += ",'" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(creationTime) + "'";
            valuesForInsert += ",'" + fiwareServicePath.replace("/", "") + "'";
            valuesForInsert += ",'" + entity.getEntityId() + "'";
            valuesForInsert += ",'" + entity.getEntityType() + "'";
            valuesForInsert += ",'" + entity.getEntityAttrs().get(i).getAttrName() + "'";
            valuesForInsert += ",'" + entity.getEntityAttrs().get(i).getAttrType() + "'";
            valuesForInsert += ",'" + entity.getEntityAttrs().get(i).getAttrValue() + "'";
            if (entity.getEntityAttrs().get(i).getAttrMetadata() != null) {
                valuesForInsert += ",'" + entity.getEntityAttrs().get(i).getMetadataString() + "'";
            } else {
                valuesForInsert += ",'[]'";
            }
            valuesForInsert += ");";

        } // for

        return valuesForInsert;
    } // getValuesForInsert


    public String buildKeyspace(String service,boolean enableEncoding,boolean enableLowercase) throws Exception {
        String name;

        if (enableEncoding) {
            name = NGSICharsets.encodeCassandra((enableLowercase)?service.toLowerCase():service);
        } else {
            name = NGSICharsets.encode((enableLowercase)?service.toLowerCase():service, false, true);
        } // if else

        if (name.length() > NGSIConstants.CASSANDRA_MAX_KEYSPACE_NAME_LEN) {
            throw new Exception("Building KEYSPACE name '" + name
                    + "' and its length is greater than " + NGSIConstants.CASSANDRA_MAX_KEYSPACE_NAME_LEN);
        } // if

        return name;
    } // buildKeyspace


    public String buildTableName(String fiwareServicePath,Entity entity, String dataModel, boolean enableEncoding, boolean enableLowercase) throws Exception {
        String name="";
        String servicePath=(enableLowercase)? fiwareServicePath.toLowerCase():fiwareServicePath;
        String entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
        String entityType = (enableLowercase) ? entity.getEntityType().toLowerCase() : entity.getEntityType();


        if (enableEncoding) {
            switch(dataModel) {
                case "db-by-service-path":
                    name = NGSICharsets.encodeCassandra(servicePath);
                    break;
                case "db-by-entity":
                    name = NGSICharsets.encodeMySQL(servicePath)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodeMySQL(entityId)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodeMySQL(entityType);
                    break;

                default:
                    System.out.println("Unknown data model '" + dataModel.toString()
                            + "'. Please, use dm-by-service-path, dm-by-entity or dm-by-attribute");
            } // switch
        } else {
            switch(dataModel) {
                case "db-by-service-path":
                    if ("/".equals(servicePath)) {
                        System.out.println("Default service path '/' cannot be used with "
                                + "dm-by-service-path data model");
                    } // if

                    name =  NGSICharsets.encode(servicePath, true, false);
                    break;
                case "db-by-entity":
                    String truncatedServicePath = NGSICharsets.encode(servicePath, true, false);
                    name = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_')
                            + NGSICharsets.encode(entityId, false, true) + "_"
                            + NGSICharsets.encode(entityType, false, true);
                    break;

                case "db-by-attribute":
                   /*truncatedServicePath = NGSICharsets.encode(servicePath, true, false);
                    name = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_')
                            + NGSICharsets.encode(entityId, false, true)
                            + '_' + NGSICharsets.encode(attribute, false, true);*/
                    break;
                default:
                    System.out.println("Unknown data model '" + dataModel.toString()
                            + "'. Please, use DMBYSERVICEPATH, DMBYENTITY or DMBYATTRIBUTE");
            } // switch
        } // if else

        if (name.length() > NGSIConstants.CASSANDRA_MAX_TABLE_NAME_LEN) {
            throw new Exception("Building table name '" + name
                    + "' and its length is greater than " + NGSIConstants.CASSANDRA_MAX_TABLE_NAME_LEN);
        } // if

        return name;
    } // buildTableName

    public String getFieldsForCreate(String attrPersistence) {
        Iterator it = listOfFields(attrPersistence).iterator();
        String fieldsForCreate = "(id UUID PRIMARY KEY, ";
        boolean first = true;
        while (it.hasNext()) {
            if (first) {
                fieldsForCreate += (String) it.next() + " text";
                first = false;
            } else {
                fieldsForCreate += "," + (String) it.next() + " text";
            } // if else
        } // while

        return fieldsForCreate + ");";
    } // getFieldsForCreate

    public String getFieldsForInsert(String attrPersistence) {
        String fieldsForInsert = "(id, ";
        boolean first = true;
        Iterator it = listOfFields(attrPersistence).iterator();

        while (it.hasNext()) {
            if (first) {
                fieldsForInsert += (String) it.next();
                first = false;
            } else {
                fieldsForInsert += "," + (String) it.next();
            } // if else
        } // while

        return fieldsForInsert + ")";
    } // getFieldsForInsert


    public String createKeyspace(String keyspace)
    {
        //Query
        String query = "CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':1};";
        return query;
    } // createDatabase

    public String createTable(String keyspace, String tableName, String attrPersistence){

        String query = "CREATE TABLE IF NOT EXISTS "+ keyspace+"."+tableName +getFieldsForCreate(attrPersistence);
        System.out.println(query);
        return query;
    }
    public String insertQuery(String keyspace, Entity entity, long creationTime, String fiwareServicePath, String tableName, String dataModel){
        String query = "INSERT INTO "+keyspace+"."+tableName +this.getFieldsForInsert(dataModel)+ " VALUES "+ this.getValuesForInsert(entity, creationTime, fiwareServicePath);
        System.out.println(query);
        return query;
    }
}
