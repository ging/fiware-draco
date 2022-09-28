package org.apache.nifi.processors.ngsi.ngsi.backends;

import org.apache.commons.math3.util.Pair;
import org.apache.nifi.processors.ngsi.ngsi.utils.*;
import org.apache.nifi.processors.ngsi.ngsi.utils.Attributes;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIConstants.POSTGRESQL_COLUMN_TYPES;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    public Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields(String attrPersistence, Entity entity, String ngsiVersion, boolean ckanCompatible, String datasetIdPrefixToTruncate) {
        Map<String, POSTGRESQL_COLUMN_TYPES> aggregation = new TreeMap<>();

        if ("v2".equals(ngsiVersion)) {
            if (ckanCompatible) {
                aggregation.put("_id", POSTGRESQL_COLUMN_TYPES.TEXT);
            }
            aggregation.put(NGSIConstants.RECV_TIME_TS, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.put(NGSIConstants.RECV_TIME, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.put(NGSIConstants.FIWARE_SERVICE_PATH, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.put(NGSIConstants.ENTITY_ID, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.put(NGSIConstants.ENTITY_TYPE, POSTGRESQL_COLUMN_TYPES.TEXT);
            if ((NGSIConstants.ATTR_PER_ROW).equalsIgnoreCase(attrPersistence)) {
                aggregation.put(NGSIConstants.ATTR_NAME, POSTGRESQL_COLUMN_TYPES.TEXT);
                aggregation.put(NGSIConstants.ATTR_TYPE, POSTGRESQL_COLUMN_TYPES.TEXT);
                aggregation.put(NGSIConstants.ATTR_VALUE, POSTGRESQL_COLUMN_TYPES.TEXT);
                aggregation.put(NGSIConstants.ATTR_MD, POSTGRESQL_COLUMN_TYPES.TEXT);
            } else if ((NGSIConstants.ATTR_PER_COLUMN).equalsIgnoreCase(attrPersistence)) {
                ArrayList<Attributes> attributes = entity.getEntityAttrs();
                if (attributes != null && !attributes.isEmpty()) {
                    for (Attributes attribute : attributes) {
                        String attrName = attribute.getAttrName();
                        aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TEXT);
                        aggregation.put(attrName + "_md", POSTGRESQL_COLUMN_TYPES.TEXT);
                    } // for
                } // if
            } //else if
        } else if ("ld".equals(ngsiVersion)) {
            Map<String, List<AttributesLD>> attributesByObservedAt = entity.getEntityAttrsLD().stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt));

            aggregation.putIfAbsent(NGSIConstants.RECV_TIME, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
            aggregation.putIfAbsent(NGSIConstants.ENTITY_ID, POSTGRESQL_COLUMN_TYPES.TEXT);
            aggregation.putIfAbsent(NGSIConstants.ENTITY_TYPE, POSTGRESQL_COLUMN_TYPES.TEXT);

            List<AttributesLD> attributesLDS = new ArrayList<>();
            attributesByObservedAt.forEach((timestamp, attributesLd) -> attributesLDS.addAll(attributesLd));
            for (AttributesLD attribute : attributesLDS) {
                String attrName = encodeAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), datasetIdPrefixToTruncate);
                if (attribute.getAttrValue() instanceof Number) {
                    if (aggregation.replace(attrName, POSTGRESQL_COLUMN_TYPES.NUMERIC) == null)
                        aggregation.putIfAbsent(attrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                } else aggregation.putIfAbsent(attrName, POSTGRESQL_COLUMN_TYPES.TEXT);
                logger.debug("Added {} in the list of fields for entity {}", attrName, entity.entityId);

                if (!attribute.observedAt.equals("")) {
                    String encodedObservedAt = encodeTimePropertyToColumnName(attrName, NGSIConstants.OBSERVED_AT);
                    aggregation.putIfAbsent(encodedObservedAt, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
                } else {
                    String encodedModifiedAt = encodeTimePropertyToColumnName(attrName, NGSIConstants.MODIFIED_AT);
                    aggregation.putIfAbsent(encodedModifiedAt, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);

                    String encodedCreatedAt = encodeTimePropertyToColumnName(attrName, NGSIConstants.CREATED_AT);
                    aggregation.putIfAbsent(encodedCreatedAt, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
                }

                if (attribute.isHasSubAttrs()) {
                    for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                        String subAttrName = subAttribute.getAttrName();
                        String encodedSubAttrName = encodeSubAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), subAttrName, datasetIdPrefixToTruncate);
                        if ("observedAt".equals(subAttrName))
                            aggregation.putIfAbsent(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
                        else if (subAttribute.getAttrValue() instanceof Number){
                            if (aggregation.replace(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.NUMERIC) == null)
                                aggregation.putIfAbsent(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                        }
                        else aggregation.putIfAbsent(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.TEXT);
                        logger.debug("Added subattribute {} ({}) to attribute {}", encodedSubAttrName, subAttrName, attrName);
                    }
                }
            }
        }
        return aggregation;
    }

    private String encodeAttributeToColumnName(String attributeName, String datasetId, String datasetIdPrefixToTruncate) {
        String encodedName = NGSIEncoders.encodePostgreSQL(attributeName) + (!datasetId.equals("") ? "_" + NGSIEncoders.encodePostgreSQL(datasetId.replaceFirst(datasetIdPrefixToTruncate, "")) : "");
        return NGSIEncoders.truncateToMaxSize(encodedName).toLowerCase();
    }

    private String encodeTimePropertyToColumnName(String encodedAttributeName, String timeProperty) {
        String encodedName = encodedAttributeName + "_" + NGSIEncoders.encodePostgreSQL(timeProperty);
        return NGSIEncoders.truncateToMaxSize(encodedName).toLowerCase();
    }

    private String encodeSubAttributeToColumnName(String attributeName, String datasetId, String subAttributeName, String datasetIdPrefixToTruncate) {
        String encodedAttributeName = encodeAttributeToColumnName(attributeName, datasetId, datasetIdPrefixToTruncate);
        String encodedName = encodedAttributeName + "_" + NGSIEncoders.encodePostgreSQL(subAttributeName);
        return NGSIEncoders.truncateToMaxSize(encodedName).toLowerCase();
    }

    public List<String> getValuesForInsert(String attrPersistence, Entity entity, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields, long creationTime, String fiwareServicePath, String ngsiVersion, boolean ckanCompatible, String datasetIdPrefixToTruncate) {
        TimeZone.setDefault(TimeZone.getTimeZone("CEST"));
        List<String> valuesForInsertList = new ArrayList<>();
        if ("v2".equals(ngsiVersion)) {
            String valuesForInsert = "";
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
            valuesForInsertList.add(valuesForInsert);
        } else if ("ld".equals(ngsiVersion)) {
            Map<String, String> valuesForColumns = new TreeMap<>();
            int i = 0;
            if (ckanCompatible) {
                valuesForColumns.put("_id", "'" + +i + "'");
            }
            Map<String, List<AttributesLD>> attributesByObservedAt = entity.getEntityAttrsLD().stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt));
            List<String> observedTimestamps = attributesByObservedAt.keySet().stream().sorted().collect(Collectors.toList());
            String oldestTimeStamp;

            if (observedTimestamps.get(0).equals("")) {
                if (observedTimestamps.size() > 1) oldestTimeStamp = observedTimestamps.get(1);
                else
                    oldestTimeStamp = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC));
            } else oldestTimeStamp = observedTimestamps.get(0);

            for (String observedTimestamp : observedTimestamps) {
                //unobserved attributes are grouped by modifiedAt timeproperty and add to columns
                    /*if(observedTimestamp.equals("")){
                        Map<String, List<AttributesLD>> attributesByModifiedAt = attributesByObservedAt.get(observedTimestamp).stream().collect(Collectors.groupingBy(attrs -> attrs.modifiedAt));
                        List<String> modifiedTimestamps = attributesByModifiedAt.keySet().stream().sorted().collect(Collectors.toList());
                        for(String modifiedTimestamp: modifiedTimestamps){
                            for (AttributesLD attribute : attributesByModifiedAt.get(modifiedTimestamp)){
                                valuesForColumns.putAll(insertAttributesValues(attribute,valuesForColumns, entity, oldestTimeStamp, listOfFields, creationTime, datasetIdPrefixToTruncate));
                            }
                            List<String> listofEncodedName = new ArrayList<>(listOfFields.keySet());
                            for (String s : listofEncodedName) {
                                valuesForColumns.putIfAbsent(s, null);
                            }
                            valuesForInsertList.add("(" + String.join(",", valuesForColumns.values()) + ")");
                        }
                    }else {
                        for (AttributesLD attribute : attributesByObservedAt.get(observedTimestamp)) {
                            valuesForColumns.putAll(insertAttributesValues(attribute,valuesForColumns, entity, oldestTimeStamp, listOfFields, creationTime, datasetIdPrefixToTruncate));
                        }
                        List<String> listofEncodedName = new ArrayList<>(listOfFields.keySet());
                        for (String s : listofEncodedName) {
                            valuesForColumns.putIfAbsent(s, null);
                        }
                        valuesForInsertList.add("(" + String.join(",", valuesForColumns.values()) + ")");
                    }*/
                for (AttributesLD attribute : attributesByObservedAt.get(observedTimestamp)) {
                    valuesForColumns.putAll(insertAttributesValues(attribute, valuesForColumns, entity, oldestTimeStamp, listOfFields, creationTime, datasetIdPrefixToTruncate));
                }
                List<String> listofEncodedName = new ArrayList<>(listOfFields.keySet());
                for (String s : listofEncodedName) {
                    valuesForColumns.putIfAbsent(s, null);
                }
                valuesForInsertList.add("(" + String.join(",", valuesForColumns.values()) + ")");
            }
        }
        return valuesForInsertList;
    }


    private Map<String, String> insertAttributesValues(AttributesLD attribute, Map<String, String> valuesForColumns, Entity entity, String oldestTimeStamp, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields, long creationTime, String datasetIdPrefixToTruncate) {
        ZonedDateTime creationDate = Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC);
        valuesForColumns.put(NGSIConstants.RECV_TIME, "'" + DateTimeFormatter.ISO_INSTANT.format(creationDate) + "'");

        valuesForColumns.put(NGSIConstants.ENTITY_ID, "'" + entity.getEntityId() + "'");
        valuesForColumns.put(NGSIConstants.ENTITY_TYPE, "'" + entity.getEntityType() + "'");

        String encodedAttributeName = encodeAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), datasetIdPrefixToTruncate);
        valuesForColumns.put(encodedAttributeName, formatFieldForValueInsert(attribute.getAttrValue(), listOfFields.get(encodedAttributeName)));

        if (!attribute.getObservedAt().equals("")){
            String encodedObservedAt = encodeTimePropertyToColumnName(encodedAttributeName, NGSIConstants.OBSERVED_AT);
            valuesForColumns.put(encodedObservedAt, formatFieldForValueInsert(attribute.getObservedAt(), listOfFields.get(encodedObservedAt)));
        }
        else {
            String encodedCreatedAt = encodeTimePropertyToColumnName(encodedAttributeName, NGSIConstants.CREATED_AT);
            if (attribute.createdAt == null || attribute.createdAt.equals("") || ZonedDateTime.parse(attribute.createdAt).toEpochSecond() > ZonedDateTime.parse(oldestTimeStamp).toEpochSecond()) {
                valuesForColumns.put(encodedCreatedAt, formatFieldForValueInsert(oldestTimeStamp, listOfFields.get(encodedCreatedAt)));
            } else
                valuesForColumns.put(encodedCreatedAt, formatFieldForValueInsert(attribute.createdAt, listOfFields.get(encodedCreatedAt)));

            String encodedModifiedAt = encodeTimePropertyToColumnName(encodedAttributeName, NGSIConstants.MODIFIED_AT);
            if (attribute.modifiedAt != null && !attribute.modifiedAt.equals("")) {
                valuesForColumns.put(encodedModifiedAt, formatFieldForValueInsert(attribute.modifiedAt, listOfFields.get(encodedModifiedAt)));
            }
        }

        if (attribute.isHasSubAttrs()) {
            for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                String encodedSubAttributeName = encodeSubAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), subAttribute.getAttrName(), datasetIdPrefixToTruncate);
                valuesForColumns.put(encodedSubAttributeName, formatFieldForValueInsert(subAttribute.getAttrValue(), listOfFields.get(encodedSubAttributeName)));
            }
        }

        return valuesForColumns;
    }


    private String formatFieldForValueInsert(Object attributeValue, POSTGRESQL_COLUMN_TYPES columnType) {
        String formattedField;
        switch (columnType) {
            case NUMERIC:
                if (attributeValue != null && (attributeValue instanceof Number)) formattedField = attributeValue.toString();
                else formattedField = null;
                break;
            case TIMESTAMPTZ:
                if (attributeValue != null) formattedField = "'" + attributeValue + "'";
                else formattedField = null;
                break;
            default:
                if (attributeValue != null) formattedField = "$$" + attributeValue + "$$";
                else formattedField = null;
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

    public String buildSchemaName(String service, boolean enableEncoding, boolean enableLowercase, boolean ckanCompatible) throws Exception {
        String dbName = "";
        if (!ckanCompatible) {
            if (enableEncoding) {
                dbName = NGSICharsets.encodePostgreSQL((enableLowercase) ? service.toLowerCase() : service);
            } else {
                dbName = NGSICharsets.encode((enableLowercase) ? service.toLowerCase() : service, false, true);
            } // if else
        } else {
            dbName = (enableLowercase) ? service.toLowerCase() : service;
        }
        if (dbName.length() > NGSIConstants.POSTGRESQL_MAX_NAME_LEN) {
            logger.error("Building database name '" + dbName + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
            throw new Exception("Building database name '" + dbName + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
        } // if
        return dbName;
    }

    public String createSchema(String schemaName) {
        return "create schema if not exists " + schemaName + ";";
    }

    public String createTable(String schemaName, String tableName, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        return "create table if not exists " + schemaName + "." + tableName + " " + getFieldsForCreate(listOfFields) + ";";
    }

    public String buildTableName(String fiwareServicePath, Entity entity, String dataModel, boolean enableEncoding, boolean enableLowercase, String ngsiVersion, boolean ckanCompatible, String attributeTableName) throws Exception {
        String tableName = "";
        String servicePath = (enableLowercase) ? fiwareServicePath.toLowerCase() : fiwareServicePath;
        String entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
        String entityType = (enableLowercase) ? entity.getEntityType().toLowerCase() : entity.getEntityType();
        if ("v2".equals(ngsiVersion)) {
            if (!ckanCompatible) {
                if (enableEncoding) {
                    switch (dataModel) {
                        case "db-by-service-path":
                            tableName = NGSICharsets.encodePostgreSQL(servicePath);
                            break;
                        case "db-by-entity":
                            tableName = NGSICharsets.encodePostgreSQL(servicePath) + CommonConstants.CONCATENATOR + NGSICharsets.encodePostgreSQL(entityId) + CommonConstants.CONCATENATOR + NGSICharsets.encodePostgreSQL(entityType);
                            break;
                        default:
                            System.out.println("Unknown data model '" + dataModel + "'. Please, use dm-by-service-path, dm-by-entity or dm-by-attribute");
                    } // switch
                } else {
                    switch (dataModel) {
                        case "db-by-service-path":
                            if ("/".equals(servicePath)) {
                                System.out.println("Default service path '/' cannot be used with " + "dm-by-service-path data model");
                            } // if

                            tableName = NGSICharsets.encode(servicePath, true, false);
                            break;
                        case "db-by-entity":
                            String truncatedServicePath = NGSICharsets.encode(servicePath, true, false);
                            tableName = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_') + NGSICharsets.encode(entityId, false, true) + "_" + NGSICharsets.encode(entityType, false, true);
                            break;
                        default:
                            System.out.println("Unknown data model '" + dataModel + "'. Please, use DMBYSERVICEPATH, DMBYENTITY or DMBYATTRIBUTE");
                            break;
                    } // switch
                } // if else
            } else {
                switch (dataModel) {
                    case "db-by-service-path":
                        if ("/".equals(servicePath)) {
                            System.out.println("Default service path '/' cannot be used with " + "dm-by-service-path data model");
                        } // if

                        tableName = servicePath;
                        break;
                    case "db-by-entity":
                        String truncatedServicePath = NGSICharsets.encode(servicePath, true, false);
                        tableName = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_') + entityId + "_" + entityType;
                        break;
                    default:
                        System.out.println("Unknown data model '" + dataModel + "'. Please, use DMBYSERVICEPATH, DMBYENTITY or DMBYATTRIBUTE");
                        break;
                } // switch
            }

            if (tableName.length() > NGSIConstants.POSTGRESQL_MAX_NAME_LEN) {
                throw new SQLException("Building table name '" + tableName + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
            } // if
        } else if ("ld".equals(ngsiVersion)) {
            if (enableEncoding) {
                switch (dataModel) {

                    case "db-by-entity":
                        tableName = NGSICharsets.encodePostgreSQL(entityId);
                        break;
                    case "db-by-entity-type":
                        if (attributeTableName != null)
                            tableName = NGSICharsets.encodePostgreSQL(entityType) + CommonConstants.OLD_CONCATENATOR + attributeTableName;
                        else tableName = NGSICharsets.encodePostgreSQL(entityType);
                        break;
                    default:
                        System.out.println("Unknown data model '" + dataModel + "'. Please, use DMBYENTITY or DMBYENTITYTYPE");
                } // switch
            } else {
                switch (dataModel) {

                    case "db-by-entity":
                        tableName = NGSIEncoders.encodePostgreSQL(entityId);
                        break;
                    case "db-by-entity-type":
                        if (attributeTableName != null)
                            tableName = NGSICharsets.encodePostgreSQL(entityType) + CommonConstants.OLD_CONCATENATOR + attributeTableName;
                        else tableName = NGSICharsets.encodePostgreSQL(entityType);
                        break;
                    default:
                        System.out.println("Unknown data model '" + dataModel + "'. Please, use DMBYENTITY or DMBYENTITYTYPE");
                } // switch
            } // if else

            if (tableName.length() > NGSIConstants.POSTGRESQL_MAX_NAME_LEN) {
                logger.error("Building table name '" + tableName + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
                throw new Exception("Building table name '" + tableName + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
            } // if
        }
        return tableName;
    }

    public String insertQuery(Entity entity, long creationTime, String fiwareServicePath, String schemaName, String tableName, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields, String dataModel, String ngsiVersion, boolean ckanCompatible, String datasetIdPrefixToTruncate) {
        List<String> valuesForInsert = this.getValuesForInsert(dataModel, entity, listOfFields, creationTime, fiwareServicePath, ngsiVersion, ckanCompatible, datasetIdPrefixToTruncate);

        return "insert into " + schemaName + "." + tableName + " " + this.getFieldsForInsert(listOfFields.keySet()) + " values " + String.join(",", valuesForInsert) + ";";
    }

    public String checkColumnNames(String tableName) {
        return "select column_name from information_schema.columns where table_name ='" + tableName + "';";
    }

    public String getColumnsTypesQuery(String tableName) {
        return "select column_name, data_type from information_schema.columns where table_name ='" + tableName + "';";
    }

    public Map<String, POSTGRESQL_COLUMN_TYPES> getUpdatedListOfTypedFields(ResultSet rs, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        // create an initial map containing all the fields with columns names in lowercase
        Map<String, POSTGRESQL_COLUMN_TYPES> newFields = listOfFields;

        try {
            // Get the column names; column indices start from 1
            while (rs.next()) {
                Pair<String, POSTGRESQL_COLUMN_TYPES> columnNameWithDataType;
                if (rs.getString(2).equals("timestamp with time zone")) {
                    columnNameWithDataType =
                            new Pair<>(rs.getString(1), POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
                } else {
                    columnNameWithDataType =
                            new Pair<>(rs.getString(1), POSTGRESQL_COLUMN_TYPES.valueOf(rs.getString(2).toUpperCase()));
                }
                if (newFields.containsKey(columnNameWithDataType.getFirst()) &&
                        newFields.get(columnNameWithDataType.getFirst()) != columnNameWithDataType.getSecond()) {
                    logger.info("Column {} with type {} already existed with a different type {}",
                            columnNameWithDataType.getFirst(),
                            newFields.get(columnNameWithDataType.getFirst()),
                            columnNameWithDataType.getSecond()
                    );
                    // update the column type to avoid type inconsistencies when inserting new values
                    // if a value in an entity does not match the current type in DB, a NULL value will be used
                    newFields.replace(columnNameWithDataType.getFirst(), columnNameWithDataType.getSecond());
                }
            }
        } catch (SQLException s) {
            logger.error("Error while inspecting columns: {}", s.getMessage(), s);
        }
        return newFields;
    }

    public Map<String, POSTGRESQL_COLUMN_TYPES> getNewColumns(ResultSet rs, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        // create an initial map containing all the fields with columns names in lowercase
        Map<String, POSTGRESQL_COLUMN_TYPES> newFields = new HashMap<>(listOfFields).entrySet().stream().map(e -> Map.entry(e.getKey().toLowerCase(), e.getValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

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
