package org.apache.nifi.processors.ngsi.ngsi.backends;

import org.apache.nifi.processors.ngsi.ngsi.utils.*;
import org.apache.nifi.processors.ngsi.ngsi.utils.Attributes;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIConstants.POSTGRESQL_COLUMN_TYPES;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class PostgreSQLBackend {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLBackend.class);

    public Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields(String attrPersistence, Entity entity, String ngsiVersion, boolean ckanCompatible) {
        Map<String, POSTGRESQL_COLUMN_TYPES> aggregation = new TreeMap<>();

        if ("v2".equals(ngsiVersion)){
            if(ckanCompatible){
                aggregation.put("_id", POSTGRESQL_COLUMN_TYPES.TEXT);
            }
            aggregation.put(NGSIConstants.RECV_TIME_TS, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.put(NGSIConstants.RECV_TIME, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.put(NGSIConstants.FIWARE_SERVICE_PATH, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.put(NGSIConstants.ENTITY_ID, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.put(NGSIConstants.ENTITY_TYPE, POSTGRESQL_COLUMN_TYPES.TEXT);
            if ((NGSIConstants.ATTR_PER_ROW).equalsIgnoreCase(attrPersistence)){
                aggregation.put(NGSIConstants.ATTR_NAME, POSTGRESQL_COLUMN_TYPES.TEXT);
                aggregation.put(NGSIConstants.ATTR_TYPE, POSTGRESQL_COLUMN_TYPES.TEXT);
                aggregation.put(NGSIConstants.ATTR_VALUE, POSTGRESQL_COLUMN_TYPES.TEXT);
                aggregation.put(NGSIConstants.ATTR_MD, POSTGRESQL_COLUMN_TYPES.TEXT);
            }else if((NGSIConstants.ATTR_PER_COLUMN).equalsIgnoreCase(attrPersistence)){
                ArrayList<Attributes> attributes = entity.getEntityAttrs();
                if (attributes != null && !attributes.isEmpty()) {
                    for (Attributes attribute : attributes) {
                        String attrName = attribute.getAttrName();
                        aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TEXT);
                        aggregation.put(attrName + "_md", POSTGRESQL_COLUMN_TYPES.TEXT);
                    } // for
                } // if
            } //else if
        }
        else if ("ld".equals(ngsiVersion)){
            aggregation.put(NGSIConstants.RECV_TIME, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
            aggregation.put(NGSIConstants.ENTITY_ID, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.put(NGSIConstants.ENTITY_TYPE, POSTGRESQL_COLUMN_TYPES.TEXT);
            if(ckanCompatible){
                aggregation.put("_id", POSTGRESQL_COLUMN_TYPES.TEXT);
            }
            ArrayList<AttributesLD> attributes = entity.getEntityAttrsLD();
            if (attributes != null && !attributes.isEmpty()) {
                for (AttributesLD attribute : attributes) {
                    String attrName = encodeAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId());
                    if (attribute.getAttrValue() instanceof BigDecimal)
                        aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                    else
                        aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TEXT);
                    logger.debug("Added {} in the list of fields for entity {}", attrName, entity.entityId);
                    if (attribute.isHasSubAttrs()) {
                        for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                            String subAttrName = subAttribute.getAttrName();
                            String encodedSubAttrName = encodeSubAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), subAttrName);
                            if ("observedAt".equals(subAttrName))
                                aggregation.put(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
                            else if (subAttribute.getAttrValue() instanceof BigDecimal)
                                aggregation.put(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                            else
                                aggregation.put(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.TEXT);
                            logger.debug("Added subattribute {} ({}) to attribute {}", encodedSubAttrName, subAttrName, attrName);
                        }
                    }
                } // for
            } // if
        }

        return aggregation;
    }

    private String encodeAttributeToColumnName(String attributeName, String datasetId) {
        String encodedName = NGSIEncoders.encodePostgreSQL(attributeName) +
                (!datasetId.equals("") ?
                        "_" + NGSIEncoders.encodePostgreSQL(datasetId.replaceFirst("urn:ngsi-ld:Dataset:", "")) : "");
        return NGSIEncoders.truncateToMaxSize(encodedName);
    }

    private String encodeSubAttributeToColumnName(String attributeName, String datasetId, String subAttributeName) {
        String encodedAttributeName = encodeAttributeToColumnName(attributeName, datasetId);
        String encodedName = encodedAttributeName + "_" + NGSIEncoders.encodePostgreSQL(subAttributeName);
        return NGSIEncoders.truncateToMaxSize(encodedName);
    }

    public String getValuesForInsert(
            String attrPersistence,
            Entity entity,
            Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields,
            long creationTime,
            String fiwareServicePath,
            String ngsiVersion,
            boolean ckanCompatible
    ) {
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
        else if ("ld".equals(ngsiVersion)) {
            Map<String, String> valuesForColumns = new TreeMap<>();
            int i = 0;
            if (ckanCompatible) {
                valuesForColumns.put("_id", "'" + +i + "'");
            }
            ZonedDateTime creationDate = Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC);
            valuesForColumns.put(NGSIConstants.RECV_TIME, "'" + DateTimeFormatter.ISO_INSTANT.format(creationDate) + "'");
            valuesForColumns.put(NGSIConstants.ENTITY_ID, "'" + entity.getEntityId() + "'");
            valuesForColumns.put(NGSIConstants.ENTITY_TYPE, "'" + entity.getEntityType() + "'");
            for (AttributesLD attribute : entity.getEntityAttrsLD()) {
                String encodedAttributeName = encodeAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId());
                valuesForColumns.put(encodedAttributeName, formatFieldForValueInsert(attribute.getAttrValue(), listOfFields.get(encodedAttributeName)));
                if (attribute.isHasSubAttrs()) {
                    for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                        String encodedSubAttributeName = encodeSubAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), subAttribute.getAttrName());
                        valuesForColumns.put(encodedSubAttributeName, formatFieldForValueInsert(subAttribute.getAttrValue(), listOfFields.get(encodedSubAttributeName)));
                  }
                }
            } //for
            valuesForInsert = "(" + String.join(",", valuesForColumns.values()) + ")";
        } // if
        return valuesForInsert;
    } // getValuesForInsert

    private String formatFieldForValueInsert(Object attributeValue, POSTGRESQL_COLUMN_TYPES columnType) {
        String formattedField;
        switch (columnType) {
            case NUMERIC: formattedField = attributeValue.toString(); break;
            default: formattedField = "'" + attributeValue.toString() + "'";
        }

        return formattedField;
    }

    public String getFieldsForCreate(Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        Iterator<Map.Entry<String, POSTGRESQL_COLUMN_TYPES>> it = listOfFields.entrySet().iterator();
        String fieldsForCreate = "(";
        boolean first = true;
        while (it.hasNext()) {
            Map.Entry<String, POSTGRESQL_COLUMN_TYPES> entry = it.next();
            if (first) {
                fieldsForCreate += entry.getKey() + " " + entry.getValue().name();
                first = false;
            } else {
                fieldsForCreate += "," + entry.getKey() + " " + entry.getValue().name();
            } // if else
        } // while

        return fieldsForCreate + ")";
    } // getFieldsForCreate

    public String getFieldsForInsert(Set<String> listOfFieldsNames) {
        return "(" + String.join(",", listOfFieldsNames) + ")";
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
        return "create schema if not exists " + schemaName + ";";
    }

    public String createTable(String schemaName, String tableName, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        return "create table if not exists "+schemaName+"." + tableName + " " + getFieldsForCreate(listOfFields) + ";";
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

    public String insertQuery(Entity entity, long creationTime, String fiwareServicePath, String schemaName,
            String tableName, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields, String dataModel, String ngsiVersion, boolean ckanCompatible) {
        return "insert into " + schemaName + "." + tableName + " " +
                this.getFieldsForInsert(listOfFields.keySet()) +
                " values " + this.getValuesForInsert(dataModel, entity, listOfFields, creationTime, fiwareServicePath, ngsiVersion, ckanCompatible);
    }

    public String checkColumnNames(String tableName){
        return "select column_name from information_schema.columns where table_name ='"+ tableName + "';";
    }

    public Map<String, POSTGRESQL_COLUMN_TYPES> getNewColumns(ResultSet rs, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        // create an initial map containing all the fields with columns names in lowercase
        Map<String, POSTGRESQL_COLUMN_TYPES> newFields = new HashMap<>(listOfFields).entrySet().stream()
                .map(e -> Map.entry(e.getKey().toLowerCase(), e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        try {
            // Get the column names; column indices start from 1
            while (rs.next()) {
                String columnName = rs.getString(1);
                logger.debug("Looking at column {} (exists: {})", columnName, newFields.containsKey(columnName));
                newFields.remove(columnName);
            }

            logger.debug("New columns to create: {}", newFields.keySet());
        } catch (SQLException s) {
            logger.error("Error while inspecting columns: {}", s.getMessage(), s);
        }

        return newFields;
    }

    public String addColumns(String schemaName, String tableName, Map<String, POSTGRESQL_COLUMN_TYPES> columnNames){
        Iterator<Map.Entry<String, POSTGRESQL_COLUMN_TYPES>> it = columnNames.entrySet().iterator();
        String fieldsForCreate = "";
        boolean first = true;
        while (it.hasNext()) {
            Map.Entry<String, POSTGRESQL_COLUMN_TYPES> entry = it.next();
            if (first) {
                fieldsForCreate += " ADD COLUMN " + entry.getKey() + " " + entry.getValue().name();
                first = false;
            } else {
                fieldsForCreate += ", ADD COLUMN " + entry.getKey() + " " + entry.getValue().name();
            } // if else
        } // while

        fieldsForCreate += ";";

        return "Alter table " + schemaName + "." + tableName + fieldsForCreate;
    }
}
