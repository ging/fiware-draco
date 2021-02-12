package org.apache.nifi.processors.ngsi.ngsi.backends;

import org.apache.nifi.processors.ngsi.ngsi.utils.*;
import org.apache.nifi.processors.ngsi.ngsi.utils.Attributes;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TimeZone;

public class PostgreSQLBackend {

    public ArrayList listOfFields (String attrPersistence, Entity entity, String ngsiVersion ,boolean ckanCompatible){
        ArrayList<String> aggregation = new ArrayList<>();
        aggregation.add(NGSIConstants.RECV_TIME);
        aggregation.add(NGSIConstants.ENTITY_ID);
        aggregation.add(NGSIConstants.ENTITY_TYPE);
        if ("v2".equals(ngsiVersion)){
            if(ckanCompatible){
                aggregation.add("_id");
            }
            aggregation.add(NGSIConstants.RECV_TIME_TS);
            aggregation.add(NGSIConstants.FIWARE_SERVICE_PATH);
            if ((NGSIConstants.ATTR_PER_ROW).equalsIgnoreCase(attrPersistence)){
                aggregation.add(NGSIConstants.ATTR_NAME);
                aggregation.add(NGSIConstants.ATTR_TYPE);
                aggregation.add(NGSIConstants.ATTR_VALUE);
                aggregation.add(NGSIConstants.ATTR_MD);
            }else if((NGSIConstants.ATTR_PER_COLUMN).equalsIgnoreCase(attrPersistence)){
                ArrayList<Attributes> attributes = entity.getEntityAttrs();
                if (attributes != null && !attributes.isEmpty()) {
                    for (Attributes attribute : attributes) {
                        String attrName = attribute.getAttrName();
                        aggregation.add(attrName);
                        aggregation.add(attrName + "_md");
                    } // for
                } // if
            } //else if
        }
        else if ("ld".equals(ngsiVersion)){
            if(ckanCompatible){
                aggregation.add("_id");
            }
                ArrayList<AttributesLD> attributes = entity.getEntityAttrsLD();
                if (attributes != null && !attributes.isEmpty()) {
                    for (AttributesLD attribute : attributes) {
                        String attrName = attribute.getAttrName();
                        aggregation.add(attrName);
                        System.out.println(attrName);
                        if (attribute.isHasSubAttrs()) {
                            for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                                String subAttrName = subAttribute.getAttrName();
                                aggregation.add(attrName + "_" + subAttrName);
                                System.out.println(attrName + "_" + subAttrName);
                            }
                        }
                    } // for
                } // if
            }

        return aggregation;
    }

    public String getValuesForInsert(String attrPersistence, Entity entity, long creationTime, String fiwareServicePath, String ngsiVersion, boolean ckanCompatible) {
        TimeZone.setDefault(TimeZone.getTimeZone("CEST"));
        String valuesForInsert = "";
        if ("v2".equals(ngsiVersion)) {
            if ((NGSIConstants.ATTR_PER_ROW).equalsIgnoreCase(attrPersistence)) {
                for (int i = 0; i < entity.getEntityAttrs().size(); i++) {
                    if (i == 0) {
                        valuesForInsert += "(";

                    } else {
                        valuesForInsert += ",(";
                    } // if else
                    if (ckanCompatible) {
                        valuesForInsert += "'" + i + "',";
                    }
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
                    valuesForInsert += ")";
                } // for
            } //if
            else if ((NGSIConstants.ATTR_PER_COLUMN).equalsIgnoreCase(attrPersistence)) {
                TimeZone.setDefault(TimeZone.getTimeZone("CEST"));
                int i = 0;
                valuesForInsert += "(";
                if (ckanCompatible) {
                    valuesForInsert += "'" + +i + "',";
                }
                valuesForInsert += "'" + creationTime + "'";
                valuesForInsert += ",'" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(creationTime) + "'";
                valuesForInsert += ",'" + fiwareServicePath.replace("/", "") + "'";
                valuesForInsert += ",'" + entity.getEntityId() + "'";
                valuesForInsert += ",'" + entity.getEntityType() + "'";
                for (Attributes attribute : entity.getEntityAttrs()) {
                    valuesForInsert += ",'" + attribute.getAttrValue() + "'";
                    if (attribute.getMetadataString() != null && !attribute.getMetadataString().isEmpty()) {
                        valuesForInsert += ",'" + attribute.getMetadataString() + "'";
                    } else {
                        valuesForInsert += ",'[]'";
                    }
                } //for
                valuesForInsert += ")";
            } //else if
        }
        else if ("ld".equals(ngsiVersion)){
                TimeZone.setDefault(TimeZone.getTimeZone("CEST"));
                int i = 0;
                valuesForInsert += "(";
                if (ckanCompatible) {
                    valuesForInsert += "'" + +i + "',";
                }
                valuesForInsert += "'" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(creationTime) + "'";
                valuesForInsert += ",'" + entity.getEntityId() + "'";
                valuesForInsert += ",'" + entity.getEntityType() + "'";
                for (AttributesLD attribute : entity.getEntityAttrsLD()) {
                    valuesForInsert += ",'" + attribute.getAttrValue() + "'";
                    if (attribute.isHasSubAttrs()) {
                        for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                            String subAttrValue = subAttribute.getAttrValue();
                            valuesForInsert += ",'" + subAttrValue + "'";
                      }
                    }
                } //for
                valuesForInsert += ")";
        } // if
        return valuesForInsert;
    } // getValuesForInsert


    public String getFieldsForCreate(String attrPersistence, Entity entity,String ngsiVersion, boolean ckanCompatible) {
        Iterator it = listOfFields(attrPersistence, entity, ngsiVersion ,ckanCompatible).iterator();
        String fieldsForCreate = "(";
        boolean first = true;
        while (it.hasNext()) {
            if (first) {
                fieldsForCreate += (String) it.next() + " text";
                first = false;
            } else {
                fieldsForCreate += "," + (String) it.next() + " text";
            } // if else
        } // while

        return fieldsForCreate + ")";
    } // getFieldsForCreate

    public String getFieldsForInsert(String attrPersistence, Entity entity,String ngsiVersion, boolean ckanCompatible) {

        String fieldsForInsert = "(";
        boolean first = true;
        Iterator it = listOfFields(attrPersistence, entity,ngsiVersion,ckanCompatible).iterator();
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

    public String buildSchemaName(String service,boolean enableEncoding,boolean enableLowercase, boolean ckanCompatible) throws Exception {
        String dbName="";
        if (!ckanCompatible) {
            if (enableEncoding) {
                dbName = NGSICharsets.encodePostgreSQL((enableLowercase) ? service.toLowerCase() : service);
            } else {
                dbName = NGSICharsets.encode((enableLowercase) ? service.toLowerCase() : service, false, true);
            } // if else
        }else {
            dbName = (enableLowercase) ? service.toLowerCase() : service;
        }
        if (dbName.length() > NGSIConstants.POSTGRESQL_MAX_NAME_LEN) {
            throw new Exception("Building database name '" + dbName
                    + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
        } // if
        return dbName;
    }

    public String createSchema(String schemaName) {
        String query = "create schema if not exists " + schemaName + ";";
        return query;
    }

    public String createTable(String schemaName,String tableName, String attrPersistence, Entity entity,String ngsiVersion,boolean ckanCompatible){

        String query= "create table if not exists "+schemaName+"." + tableName + " " + getFieldsForCreate(attrPersistence, entity, ngsiVersion,ckanCompatible) + ";";
        return query;
    }

    public String buildTableName(String fiwareServicePath,Entity entity, String dataModel, boolean enableEncoding, boolean enableLowercase, String ngsiVersion, boolean ckanCompatible)throws Exception{
        String tableName="";
        String servicePath=(enableLowercase)?fiwareServicePath.toLowerCase():fiwareServicePath;
        String entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
        String entityType = (enableLowercase) ? entity.getEntityType().toLowerCase() : entity.getEntityType();
        if ("v2".equals(ngsiVersion)){
            if (!ckanCompatible) {
                if (enableEncoding) {
                    switch (dataModel) {
                        case "db-by-service-path":
                            tableName = NGSICharsets.encodePostgreSQL(servicePath);
                            break;
                        case "db-by-entity":
                            tableName = NGSICharsets.encodePostgreSQL(servicePath)
                                    + CommonConstants.CONCATENATOR
                                    + NGSICharsets.encodePostgreSQL(entityId)
                                    + CommonConstants.CONCATENATOR
                                    + NGSICharsets.encodePostgreSQL(entityType);
                            break;
                        default:
                            System.out.println("Unknown data model '" + dataModel
                                    + "'. Please, use dm-by-service-path, dm-by-entity or dm-by-attribute");
                    } // switch
                } else {
                    switch (dataModel) {
                        case "db-by-service-path":
                            if ("/".equals(servicePath)) {
                                System.out.println("Default service path '/' cannot be used with "
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
                            System.out.println("Unknown data model '" + dataModel
                                    + "'. Please, use DMBYSERVICEPATH, DMBYENTITY or DMBYATTRIBUTE");
                            break;
                    } // switch
                } // if else
            }
            else{
                switch (dataModel) {
                    case "db-by-service-path":
                        if ("/".equals(servicePath)) {
                            System.out.println("Default service path '/' cannot be used with "
                                    + "dm-by-service-path data model");
                        } // if

                        tableName = servicePath;
                        break;
                    case "db-by-entity":
                        String truncatedServicePath = NGSICharsets.encode(servicePath, true, false);
                        tableName = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_')
                                + entityId+ "_"
                                + entityType;
                        break;
                    default:
                        System.out.println("Unknown data model '" + dataModel
                                + "'. Please, use DMBYSERVICEPATH, DMBYENTITY or DMBYATTRIBUTE");
                        break;
                } // switch
            }

            if (tableName.length() > NGSIConstants.POSTGRESQL_MAX_NAME_LEN) {
                throw new SQLException("Building table name '" + tableName
                        + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
            } // if
        }
        else if ("ld".equals(ngsiVersion)){
            if (enableEncoding) {
                switch(dataModel) {

                    case "db-by-entity":
                        tableName = NGSICharsets.encodePostgreSQL(entityId);
                        break;
                    case "db-by-entity-type":
                        tableName = NGSICharsets.encodePostgreSQL(entityType);
                        break;
                    default:
                        System.out.println("Unknown data model '" + dataModel
                                + "'. Please, use DMBYENTITY or DMBYENTITYTYPE");
                } // switch
            } else {
                switch(dataModel) {

                    case "db-by-entity":
                        tableName = NGSIEncoders.encodePostgreSQL(entityId);
                        break;
                    case "db-by-entity-type":
                        tableName = (NGSIEncoders.encodePostgreSQL(entityType));

                        break;
                    default:
                        System.out.println("Unknown data model '" + dataModel
                                + "'. Please, use DMBYENTITY or DMBYENTITYTYPE");
                } // switch
            } // if else

            if (tableName.length() > NGSIConstants.POSTGRESQL_MAX_NAME_LEN) {
                System.out.println("Building table name '" + tableName
                        + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
            } // if
        }
        return tableName;
    }

    public String insertQuery (Entity entity, long creationTime, String fiwareServicePath, String schemaName, String tableName, String dataModel, String ngsiVersion, boolean ckanCopatible){
        String query="Insert into "+schemaName+"."+ tableName + " " +this.getFieldsForInsert(dataModel, entity, ngsiVersion,ckanCopatible)+ " values " +this.getValuesForInsert(dataModel, entity, creationTime, fiwareServicePath,ngsiVersion,ckanCopatible);
        return query;
    }
}
