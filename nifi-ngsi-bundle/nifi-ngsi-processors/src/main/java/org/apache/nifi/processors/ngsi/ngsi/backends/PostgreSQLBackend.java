package org.apache.nifi.processors.ngsi.ngsi.backends;

import javafx.util.Pair;
import org.apache.nifi.processors.ngsi.ngsi.utils.*;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIConstants.POSTGRESQL_COLUMN_TYPES;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class PostgreSQLBackend {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLBackend.class);

    public Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields(
            Entity entity,
            String prefixToTruncate
    ) {
        Map<String, POSTGRESQL_COLUMN_TYPES> aggregation = new TreeMap<>();

        //Map<String, Pair<String, POSTGRESQL_COLUMN_TYPES>> aggr = new Pair<>()

        Map<String, List<AttributesLD>> attributesByObservedAt = entity.getEntityAttrsLD().stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt));

        aggregation.putIfAbsent(NGSIConstants.RECV_TIME, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
        aggregation.putIfAbsent(NGSIConstants.ENTITY_ID, POSTGRESQL_COLUMN_TYPES.TEXT);
        aggregation.putIfAbsent(NGSIConstants.ENTITY_TYPE, POSTGRESQL_COLUMN_TYPES.TEXT);

        for(String timeStamp: attributesByObservedAt.keySet()){
            List<AttributesLD> attributes = attributesByObservedAt.get(timeStamp);
            if (attributes != null && !attributes.isEmpty()) {
                for (AttributesLD attribute : attributes) {

                    String attrName = encodeAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), prefixToTruncate);
                    if (attribute.getAttrValue() instanceof BigDecimal)
                        aggregation.putIfAbsent(attrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                    else
                        aggregation.putIfAbsent(attrName, POSTGRESQL_COLUMN_TYPES.TEXT);

                    logger.debug("Added {} in the list of fields for entity {}", attrName, entity.entityId);

                    String encodedObservedAt = encodeObservedAtToColumnName(attrName, NGSIConstants.OBSERVED_AT, prefixToTruncate);
                    aggregation.putIfAbsent(encodedObservedAt, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);

                    if (attribute.isHasSubAttrs()) {
                        for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                            String subAttrName = subAttribute.getAttrName();
                            String encodedSubAttrName = encodeSubAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), subAttrName, prefixToTruncate);
                            if ("observedAt".equals(subAttrName))
                                aggregation.putIfAbsent(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
                            else if (subAttribute.getAttrValue() instanceof BigDecimal)
                                aggregation.putIfAbsent(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                            else
                                aggregation.putIfAbsent(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.TEXT);
                            logger.debug("Added subattribute {} ({}) to attribute {}", encodedSubAttrName, subAttrName, attrName);
                        }
                    }
                }
            }
        }

        return aggregation;
    }

    private String encodeAttributeToColumnName(String attributeName, String datasetId, String datasetIdPrefixToTruncate) {
        String encodedName = NGSIEncoders.encodePostgreSQL(attributeName) +
                (!datasetId.equals("") ?
                        "_" + NGSIEncoders.encodePostgreSQL(datasetId.replaceFirst(datasetIdPrefixToTruncate, "")) : "");
        return NGSIEncoders.truncateToMaxSize(encodedName);
    }

    private String encodeSubAttributeToColumnName(String attributeName, String datasetId, String subAttributeName, String datasetIdPrefixToTruncate) {
        String encodedAttributeName = encodeAttributeToColumnName(attributeName, datasetId, datasetIdPrefixToTruncate);
        String encodedName = encodedAttributeName + "_" + NGSIEncoders.encodePostgreSQL(subAttributeName);
        return NGSIEncoders.truncateToMaxSize(encodedName);
    }

    private String encodeObservedAtToColumnName(String encodedAttributeName, String observedAt, String observedAtPrefixToTruncate) {
        String encodedName = encodedAttributeName + "_" + NGSIEncoders.encodePostgreSQL(observedAt);
        return NGSIEncoders.truncateToMaxSize(encodedName);
    }

    public List<String> getValuesForInsert(
            Entity entity,
            Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields,
            long creationTime,
            String prefixToTruncate
    ) {
        TimeZone.setDefault(TimeZone.getTimeZone("CEST"));
        List<String> valuesForInsert = new ArrayList<>();
        Map<String, String> valuesForColumns = new TreeMap<>();

        Map<String, List<AttributesLD>> attributesByObservedAt = entity.getEntityAttrsLD().stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt));
        for(String timeStamp: attributesByObservedAt.keySet()){
            for (AttributesLD attribute : attributesByObservedAt.get(timeStamp)) {
                ZonedDateTime creationDate = Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC);

                ZonedDateTime observedAt = ZonedDateTime.parse(attribute.observedAt);
                if(creationDate.toEpochSecond()>observedAt.toEpochSecond())
                    valuesForColumns.put(NGSIConstants.RECV_TIME, "'" + DateTimeFormatter.ISO_INSTANT.format(observedAt) + "'");
                else valuesForColumns.put(NGSIConstants.RECV_TIME, "'" + DateTimeFormatter.ISO_INSTANT.format(creationDate) + "'");

                valuesForColumns.put(NGSIConstants.ENTITY_ID, "'" + entity.getEntityId() + "'");
                valuesForColumns.put(NGSIConstants.ENTITY_TYPE, "'" + entity.getEntityType() + "'");

                String encodedAttributeName = encodeAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), prefixToTruncate);
                valuesForColumns.put(encodedAttributeName, formatFieldForValueInsert(attribute.getAttrValue(), listOfFields.get(encodedAttributeName)));

                String encodedObservedAt = encodeObservedAtToColumnName(encodedAttributeName, NGSIConstants.OBSERVED_AT, prefixToTruncate);
                valuesForColumns.put(encodedObservedAt, formatFieldForValueInsert(attribute.getObservedAt(), listOfFields.get(encodedObservedAt)));
                if (attribute.isHasSubAttrs()) {
                    for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                        String encodedSubAttributeName = encodeSubAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), subAttribute.getAttrName(), prefixToTruncate);
                        valuesForColumns.put(encodedSubAttributeName, formatFieldForValueInsert(subAttribute.getAttrValue(), listOfFields.get(encodedSubAttributeName)));
                    }
                }
            }
            valuesForInsert.add("(" + String.join(",", valuesForColumns.values()) + ")");//for
        }
        return valuesForInsert;
    } // getValuesForInsert

    private String formatFieldForValueInsert(Object attributeValue, POSTGRESQL_COLUMN_TYPES columnType) {
        String formattedField;
        switch (columnType) {
            case NUMERIC:
                formattedField = attributeValue.toString();
                break;
            default:
                formattedField = "'" + attributeValue.toString() + "'";
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

    public String buildSchemaName(String service, boolean enableEncoding, boolean enableLowercase) throws Exception {
        String dbName = "";
        if (enableEncoding) {
            dbName = NGSICharsets.encodePostgreSQL((enableLowercase) ? service.toLowerCase() : service);
        } else {
            dbName = NGSICharsets.encode((enableLowercase) ? service.toLowerCase() : service, false, true);
        } // if else
//        if (!ckanCompatible) {
//
//        } else {
//            dbName = (enableLowercase) ? service.toLowerCase() : service;
//        }
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
        return "create table if not exists " + schemaName + "." + tableName + " " + getFieldsForCreate(listOfFields) + ";";
    }

    public String buildTableName(Entity entity, String dataModel, boolean enableEncoding, boolean enableLowercase) throws Exception {
        String tableName = "";
        String entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
        String entityType = (enableLowercase) ? entity.getEntityType().toLowerCase() : entity.getEntityType();

        if (enableEncoding) {
            switch (dataModel) {
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
            switch (dataModel) {
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
        }
        return tableName;
    }

    public String insertQuery(
            Entity entity,
            long creationTime,
            String schemaName,
            String tableName,
            Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields,
            String datasetIdPrefixToTruncate) {
        StringBuilder instertQueryValue  = new StringBuilder();
        List<String> valuesForInsert = this.getValuesForInsert(entity, listOfFields, creationTime,datasetIdPrefixToTruncate);
        for(String valueForInstert: valuesForInsert){
            instertQueryValue.append(
                    "insert into " + schemaName + "." + tableName + " " +
                    this.getFieldsForInsert(listOfFields.keySet()) +
                    " values " +valueForInstert + "\n");
        }
        return instertQueryValue.toString();
    }

    public String checkColumnNames(String tableName) {
        return "select column_name from information_schema.columns where table_name ='" + tableName + "';";
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

    public String addColumns(String schemaName, String tableName, Map<String, POSTGRESQL_COLUMN_TYPES> columnNames) {
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
