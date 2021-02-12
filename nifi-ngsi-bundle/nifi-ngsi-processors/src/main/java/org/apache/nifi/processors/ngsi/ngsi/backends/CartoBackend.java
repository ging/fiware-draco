package org.apache.nifi.processors.ngsi.ngsi.backends;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.nifi.processors.ngsi.ngsi.utils.*;
import org.apache.nifi.processors.ngsi.NGSIToCarto;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;

public class CartoBackend {

    public CartoBackend() {
    }
    private static final NGSIToCarto ngsiTo = new NGSIToCarto();

    public ArrayList listOfFields (String attrPersistence, Entity entity){
        ArrayList<String> aggregation = new ArrayList<>();
        if (attrPersistence.compareToIgnoreCase("raw")==0){

            aggregation.add("cartodb_id");
            aggregation.add(NGSIConstants.RECV_TIME_TS);
            aggregation.add(NGSIConstants.RECV_TIME);
            aggregation.add(NGSIConstants.FIWARE_SERVICE_PATH);
            aggregation.add(NGSIConstants.ENTITY_ID);
            aggregation.add(NGSIConstants.ENTITY_TYPE);

            for(int i=0; i<entity.getEntityAttrs().size(); i++){
                if (entity.getEntityAttrs().get(i).getAttrType().compareToIgnoreCase("geo:point") == 0 || entity.getEntityAttrs().get(i).getAttrType().compareToIgnoreCase("geo:json") == 0) {
                    aggregation.add(NGSIConstants.CARTO_DB_THE_GEOM);
                    aggregation.add("the_geom_webmercator");
                }
            }
            for(int i=0; i<entity.getEntityAttrs().size(); i++){
                if(entity.getEntityAttrs().get(i).attrName.compareToIgnoreCase("location") !=0){
                    aggregation.add(entity.getEntityAttrs().get(i).getAttrName());
                    aggregation.add(entity.getEntityAttrs().get(i).getAttrName() + "_md");
                }
            }


           /*aggregation.add(NGSIConstants.ATTR_MD);*/

        }else if(attrPersistence.compareToIgnoreCase("distance")==0){

            aggregation.add("cartodb_id");
            aggregation.add(NGSIConstants.RECV_TIME_TS);
            aggregation.add(NGSIConstants.FIWARE_SERVICE_PATH);
            aggregation.add(NGSIConstants.ENTITY_ID);
            aggregation.add(NGSIConstants.ENTITY_TYPE);
            aggregation.add(NGSIConstants.CARTO_DB_THE_GEOM);
            aggregation.add("the_geom_webmercator");
            aggregation.add("stageDistance");
            aggregation.add("stageTime");
            aggregation.add("stageSpeed");
            aggregation.add("sumDistance");
            aggregation.add("sumTime");
            aggregation.add("sumSpeed");
            aggregation.add("sum2Distance");
            aggregation.add("sum2Time");
            aggregation.add("sum2Speed");
            aggregation.add("maxDistance");
            aggregation.add("minDistance");
            aggregation.add("maxTime");
            aggregation.add("minTime");
            aggregation.add("maxSpeed");
            aggregation.add("minSpeed");
            aggregation.add("numSamples");

        }
        return aggregation;
    }

    public String getValuesForInsert(Entity entity, long creationTime, String fiwareServicePath, ImmutablePair<String, Boolean> imm,ImmutablePair<String, Boolean> immWeb, String CasoVacia) {
        String valuesForInsert = "";
        boolean first = true;

        //Por cada atributo tenemos que dar un id, type,etc.


        valuesForInsert += "(";
        valuesForInsert += (CasoVacia.compareToIgnoreCase("enable") == 0) ? "1," : "(SELECT idf FROM id),";
        valuesForInsert += "'" + creationTime + "'";
        valuesForInsert += ",'" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(creationTime) + "'";
        valuesForInsert += ",'" + fiwareServicePath.replace("/", "") + "'";
        valuesForInsert += ",'" + entity.getEntityId() + "'";
        valuesForInsert += ",'" + entity.getEntityType() + "'";

        for (int i = 0; i < entity.getEntityAttrs().size(); i++) {
            if (entity.getEntityAttrs().get(i).getAttrType().compareToIgnoreCase("geo:point") == 0 || entity.getEntityAttrs().get(i).getAttrType().compareToIgnoreCase("geo:json") == 0) {
                String attrValue = entity.getEntityAttrs().get(i).getAttrValue();
                String attrType = entity.getEntityAttrs().get(i).getAttrType();
                ImmutablePair<String, Boolean> geo = ngsiTo.getGeom(attrType, attrValue, false);
                ImmutablePair<String, Boolean> geoweb = ngsiTo.getGeomWebmercator(attrType, attrValue, false);
                valuesForInsert += "," + geo.getLeft() + "";
                valuesForInsert += "," + geoweb.getLeft() + "";
            }
        }
        for (int i = 0; i < entity.getEntityAttrs().size(); i++) {
            if( entity.getEntityAttrs().get(i).attrName.compareToIgnoreCase("location") !=0) {
                valuesForInsert += ",'" + entity.getEntityAttrs().get(i).getAttrValue() + "'";
                if (entity.getEntityAttrs().get(i).getAttrMetadata() != null) {
                    valuesForInsert += ",'" + entity.getEntityAttrs().get(i).getMetadataString() + "'";
                } else {
                    valuesForInsert += ",'[]'";
                }
            }

        }

            valuesForInsert += ")";

            // for

            return valuesForInsert;
        } // getValuesForInsert


    public String getFieldsForInsert(String attrPersistence, Entity entity) {

        String fieldsForInsert = "(";
        boolean first = true;
        Iterator it = listOfFields(attrPersistence, entity).iterator();
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

    public String buildSchemaName(String service,boolean enableEncoding,boolean enableLowercase) throws Exception {
        String dbName="";
        if (enableEncoding) {
            dbName = NGSICharsets.encodePostgreSQL((enableLowercase)?service.toLowerCase():service);
        } else {
            dbName = NGSICharsets.encode((enableLowercase)?service.toLowerCase():service, false, true);
        } // if else

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


    public String createTable(String schemaName,String tableName, String attrPersistence, String typeOfFields, Entity entity){
        String query="";
        String fieldsForInsert = "(";
        fieldsForInsert += "cartodb_id integer";
        fieldsForInsert += ",recvtimets text";
        fieldsForInsert += ",recvtime text";
        fieldsForInsert += ",fiwareservicepath text";
        fieldsForInsert += ",entityid text";
        fieldsForInsert += ",entitytype text";
        for(int i=0; i<entity.getEntityAttrs().size();i++) {
            if (entity.getEntityAttrs().get(i).getAttrType().compareToIgnoreCase("geo:point") == 0 || entity.getEntityAttrs().get(i).getAttrType().compareToIgnoreCase("geo:json") == 0) {

                fieldsForInsert += ",the_geom text";
                fieldsForInsert += ",the_geom_webmercator text";
            }
        }
        for(int i=0; i<entity.getEntityAttrs().size();i++){
            if (entity.getEntityAttrs().get(i).attrName.compareToIgnoreCase("location") !=0) {

                fieldsForInsert += "," + entity.getEntityAttrs().get(i).getAttrName() + " text";
                fieldsForInsert += "," + entity.getEntityAttrs().get(i).getAttrName() + "_md text";

            }
        }
        fieldsForInsert += ")";
            if(attrPersistence.compareToIgnoreCase("distance")==0) {
                query = "create table if not exists " + schemaName + "." + tableName + " " + typeOfFields + ";";
            } else {
                query = "create table if not exists " + schemaName + "." + tableName + " " + fieldsForInsert + ";";
            }

        return query;
    }
    public String deleteTable(String tableName){
        String query = "DROP TABLE if exists" + tableName + "" +";";
        return query;
    }
    public String nRows(String schemaName, String tableName){
        String query = "SELECT COUNT(*) FROM " + schemaName + "." + tableName;
        return query;
    }

    public String buildTableName(String fiwareServicePath,Entity entity, String dataModel, boolean enableEncoding, boolean enableLowercase)throws Exception{
        String tableName="";
        String servicePath=(enableLowercase)?fiwareServicePath.toLowerCase():fiwareServicePath;
        String entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
        String entityType = (enableLowercase) ? entity.getEntityType().toLowerCase() : entity.getEntityType();

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
                    System.out.println("Unknown data model '" + dataModel.toString()
                            + "'. Please, use dm-by-service-path, dm-by-entity or dm-by-attribute");
            } // switch
        } else {
            switch (dataModel) {
                case "db-by-service-path":
                    if (servicePath.equals("/")) {
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
                    System.out.println("Unknown data model '" + dataModel.toString()
                            + "'. Please, use DMBYSERVICEPATH, DMBYENTITY or DMBYATTRIBUTE");
            } // switch
        } // if else

        if (tableName.length() > NGSIConstants.POSTGRESQL_MAX_NAME_LEN) {
            throw new Exception("Building table name '" + tableName
                    + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
        } // if
        return tableName;
    }

    public String insertQuery (Entity entity, long creationTime, String fiwareServicePath, String schemaName, String tableName, String dataModel, String withs, ImmutablePair<String, Boolean> imm, String rows, ImmutablePair<String, Boolean> immWeb, String rows2raw){
        String query ="";

        boolean geo= imm.getRight();
        if(dataModel.compareToIgnoreCase("raw")==0) {
                String withsf = (rows2raw=="enable")?"":withs;
                query = withsf + "Insert into " + schemaName + "." + tableName + " " + this.getFieldsForInsert(dataModel, entity)
                        + " values " + this.getValuesForInsert(entity, creationTime, fiwareServicePath, imm, immWeb, rows2raw);

        }else if(dataModel.compareToIgnoreCase("distance")==0){

            query = withs + "Insert into " + schemaName + "." + tableName + " " +this.getFieldsForInsert(dataModel, entity) + " values " + rows;

        }
        return query;
    }
}
