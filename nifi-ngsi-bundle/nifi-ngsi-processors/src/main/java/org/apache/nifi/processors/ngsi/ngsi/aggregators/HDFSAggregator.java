package org.apache.nifi.processors.ngsi.ngsi.aggregators;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.processors.ngsi.ngsi.backends.hdfs.HDFSBackend;
import org.apache.nifi.processors.ngsi.ngsi.backends.HiveBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public abstract class HDFSAggregator {

    private String csvSeparator=",";
    protected String aggregation;
    // map containing the HDFS files holding the attribute metadata, one per attribute
    protected Map<String, String> mdAggregations;
    protected String service;
    protected String servicePath;
    protected String destination;
    protected String hdfsFolder;
    protected String hdfsFile;
    protected String hiveFields;

    public HDFSAggregator() {
        aggregation = "";
        mdAggregations = new HashMap<String, String>();
    } // HDFSAggregator

    public String getAggregation() {
        return aggregation;
    } // getAggregation

    public Set<String> getAggregatedAttrMDFiles() {
        return mdAggregations.keySet();
    } // getAggregatedAttrMDFiles

    public String getMDAggregation(String attrMDFile) {
        return mdAggregations.get(attrMDFile);
    } // getMDAggregation

    public String getFolder(boolean enableLowercase) {
        if (enableLowercase) {
            return hdfsFolder.toLowerCase();
        } else {
            return hdfsFolder;
        } // if else
    } // getFolder

    public String getFile(boolean enableLowercase) {
        if (enableLowercase) {
            return hdfsFile.toLowerCase();
        } else {
            return hdfsFile;
        } // if else
    } // getFile

    public String getHiveFields() {
        return hiveFields;
    } // getHiveFields

    public String getService(){
        return service;
    }

    public void initialize(String fiwareService,String fiwareServicePath, Entity entity,boolean enableEncoding) throws Exception {
        service = fiwareService;
        servicePath = fiwareServicePath;
        destination = entity.getEntityId();
        hdfsFolder = buildFolderPath(service, servicePath, destination,enableEncoding);
        hdfsFile = buildFilePath(service, servicePath, destination,enableEncoding);
    } // initialize

    public abstract void aggregate(long creationTime,Entity entity,String username) throws Exception;

    // HDFSAggregator

    /**
     * Class for aggregating batches in JSON row mode.
     */
    private class JSONRowAggregator extends HDFSAggregator {

        @Override
        public void initialize(String fiwareService, String fiwareServicePath, Entity entity, boolean enableEncoding) throws Exception {
            super.initialize(fiwareService, fiwareServicePath, entity, enableEncoding);
            hiveFields = NGSICharsets.encodeHive(NGSIConstants.RECV_TIME_TS) + " bigint,"
                    + NGSICharsets.encodeHive(NGSIConstants.RECV_TIME) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.FIWARE_SERVICE_PATH) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ENTITY_ID) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ENTITY_TYPE) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ATTR_NAME) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ATTR_TYPE) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ATTR_VALUE) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ATTR_MD)
                    + " array<struct<name:string,type:string,value:string>>";
        } // initialize

        @Override
        public void aggregate(long creationTime,Entity entity,String username) throws Exception {
            // get the event headers
            long recvTimeTs = creationTime;
            String recvTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(recvTimeTs);

            // get the event body
            String entityId = entity.getEntityId();
            String entityType = entity.getEntityType();
            System.out.println("[Processing context element (id=" + entityId + ", type="
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
                System.out.println(" Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");

                // create a line and aggregate it
                String line = "{"
                        + "\"" + NGSIConstants.RECV_TIME_TS + "\":\"" + recvTimeTs / 1000 + "\","
                        + "\"" + NGSIConstants.RECV_TIME + "\":\"" + recvTime + "\","
                        + "\"" + NGSIConstants.FIWARE_SERVICE_PATH + "\":\"" + servicePath + "\","
                        + "\"" + NGSIConstants.ENTITY_ID + "\":\"" + entityId + "\","
                        + "\"" + NGSIConstants.ENTITY_TYPE + "\":\"" + entityType + "\","
                        + "\"" + NGSIConstants.ATTR_NAME + "\":\"" + attrName + "\","
                        + "\"" + NGSIConstants.ATTR_TYPE + "\":\"" + attrType + "\","
                        + "\"" + NGSIConstants.ATTR_VALUE + "\":" + attrValue + ","
                        + "\"" + NGSIConstants.ATTR_MD + "\":" + attrMetadata
                        + "}";

                if (aggregation.isEmpty()) {
                    aggregation = line;
                } else {
                    aggregation += "\n" + line;
                } // if else
            } // for
        } // aggregate

    } // JSONRowAggregator

    /**
     * Class for aggregating batches in JSON column mode.
     */
    private class JSONColumnAggregator extends HDFSAggregator {

        @Override
        public void initialize(String fiwareService,String fiwareServicePath, Entity entity, boolean enableEncoding) throws Exception {
            super.initialize(fiwareService, fiwareServicePath, entity, enableEncoding);

            // particular initialization
            hiveFields = NGSICharsets.encodeHive(NGSIConstants.RECV_TIME) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.FIWARE_SERVICE_PATH) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ENTITY_ID) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ENTITY_TYPE) + " string";

            // iterate on all this context element attributes, if there are attributes
            ArrayList<Attributes> attributes = entity.getEntityAttrs();

            if (attributes == null || attributes.isEmpty()) {
                return;
            } // if

            for (Attributes attribute : attributes) {
                String attrName = attribute.getAttrName();
                hiveFields += "," + NGSICharsets.encodeHive(attrName) + " string," + NGSICharsets.encodeHive(attrName)
                        + "_md array<struct<name:string,type:string,value:string>>";
            } // for
        } // initialize

        @Override
        public void aggregate(long creationTime,Entity entity,String username) throws Exception {
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

            String line = "{\"" + NGSIConstants.RECV_TIME + "\":\"" + recvTime + "\","
                    + "\"" + NGSIConstants.FIWARE_SERVICE_PATH + "\":\"" + servicePath + "\","
                    + "\"" + NGSIConstants.ENTITY_ID + "\":\"" + entityId + "\","
                    + "\"" + NGSIConstants.ENTITY_TYPE + "\":\"" + entityType + "\"";

            for (Attributes attribute : attributes) {
                String attrName = attribute.getAttrName();
                String attrType = attribute.getAttrType();
                String attrValue = attribute.getAttrValue();
                String attrMetadata = attribute.getMetadataString();
                System.out.println("Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");

                // create part of the line with the current attribute (a.k.a. a column)
                line += ", \"" + attrName + "\":" + attrValue + ", \"" + attrName + "_md\":" + attrMetadata;
            } // for

            // now, aggregate the line
            if (aggregation.isEmpty()) {
                aggregation = line + "}";
            } else {
                aggregation += "\n" + line + "}";
            } // if else
        } // aggregate

    } // JSONColumnAggregator

    /**
     * Class for aggregating batches in CSV row mode.
     */
    private class CSVRowAggregator extends HDFSAggregator {

        @Override
        public void initialize(String fiwareService,String fiwareServicePath, Entity entity, boolean enableEncoding) throws Exception {
            super.initialize(fiwareService, fiwareServicePath, entity,enableEncoding);
            hiveFields = NGSICharsets.encodeHive(NGSIConstants.RECV_TIME_TS) + " bigint,"
                    + NGSICharsets.encodeHive(NGSIConstants.RECV_TIME) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.FIWARE_SERVICE_PATH) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ENTITY_ID) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ENTITY_TYPE) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ATTR_NAME) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ATTR_TYPE) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ATTR_VALUE) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ATTR_MD_FILE) + " string";
        } // initialize

        @Override
        public void aggregate(long creationTime,Entity entity,String username) throws Exception {
            // get the event headers
            long recvTimeTs = creationTime;
            String recvTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(recvTimeTs);

            // get the event body
            String entityId = entity.getEntityId();
            String entityType = entity.getEntityType();
            System.out.println(" Processing context element (id=" + entityId + ", type="
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
                System.out.println("[Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");
                // this has to be done notification by notification and not at initialization since in row mode not all
                // the notifications contain all the attributes
                String attrMdFileName = buildAttrMdFilePath(service, servicePath, destination, attrName, attrType);
                String printableAttrMdFileName = "hdfs:///user/" + username + "/" + attrMdFileName;
                String mdAggregation = mdAggregations.get(attrMdFileName);

                if (mdAggregation == null) {
                    mdAggregation = new String();
                } // if

                // aggregate the metadata
                String concatMdAggregation;

                if (mdAggregation.isEmpty()) {
                    concatMdAggregation = getCSVMetadata(attrMetadata, recvTimeTs);
                } else {
                    concatMdAggregation = mdAggregation.concat("\n" + getCSVMetadata(attrMetadata, recvTimeTs));
                } // if else

                mdAggregations.put(attrMdFileName, concatMdAggregation);

                // aggreagate the data
                String line = recvTimeTs / 1000 + csvSeparator
                        + recvTime + csvSeparator
                        + servicePath + csvSeparator
                        + entityId + csvSeparator
                        + entityType + csvSeparator
                        + attrName + csvSeparator
                        + attrType + csvSeparator
                        + attrValue.replaceAll("\"", "") + csvSeparator
                        + printableAttrMdFileName;
                if (aggregation.isEmpty()) {
                    aggregation = line;
                } else {
                    aggregation += "\n" + line;
                } // if else
            } // for
        } // aggregate

        private String getCSVMetadata(String attrMetadata, long recvTimeTs) throws Exception {
            String csvMd = "";

            // metadata is in JSON format, decode it
            JSONParser jsonParser = new JSONParser();
            JSONArray attrMetadataJSON = (JSONArray) jsonParser.parse(attrMetadata);

            // iterate on the metadata
            for (Object mdObject : attrMetadataJSON) {
                JSONObject mdJSONObject = (JSONObject) mdObject;
                String line = recvTimeTs + ","
                        + mdJSONObject.get("name") + ","
                        + mdJSONObject.get("type") + ","
                        + mdJSONObject.get("value");

                if (csvMd.isEmpty()) {
                    csvMd = line;
                } else {
                    csvMd += "\n" + line;
                } // if else
            } // for

            return csvMd;
        } // getCSVMetadata

    } // CSVRowAggregator

    /**
     * Class for aggregating aggregation in CSV column mode.
     */
    private class CSVColumnAggregator extends HDFSAggregator {

        @Override
        public void initialize(String fiwareService,String fiwareServicePath, Entity entity, boolean enableEncoding) throws Exception {
            super.initialize(fiwareService, fiwareServicePath, entity, enableEncoding);

            // particular initialization
            hiveFields = NGSICharsets.encodeHive(NGSIConstants.RECV_TIME) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.FIWARE_SERVICE_PATH) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ENTITY_ID) + " string,"
                    + NGSICharsets.encodeHive(NGSIConstants.ENTITY_TYPE) + " string";

            // iterate on all this context element attributes; it is supposed all the entity's attributes are notified
            ArrayList<Attributes> attributes = entity.getEntityAttrs();

            if (attributes == null || attributes.isEmpty()) {
                return;
            } // if

            for (Attributes attribute : attributes) {
                String attrName = attribute.getAttrName();
                String attrType = attribute.getAttrType();
                String attrMdFileName = buildAttrMdFilePath(service, servicePath, destination, attrName, attrType);
                mdAggregations.put(attrMdFileName, new String());
                hiveFields += ",`" + NGSICharsets.encodeHive(attrName) + "` string,"
                        + "`" + NGSICharsets.encodeHive(attrName) + "_md_file` string";
            } // for
        } // initialize

        @Override
        public void aggregate(long creationTime,Entity entity,String username) throws Exception {
            // get the event headers
            long recvTimeTs = creationTime;
            String recvTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(recvTimeTs);

            // get the event body
            String entityId = entity.getEntityId();
            String entityType = entity.getEntityType();
            System.out.println(" Processing context element (id=" + entityId + ", type="
                    + entityType + ")");

            // iterate on all this context element attributes, if there are attributes
            ArrayList<Attributes> attributes = entity.getEntityAttrs();

            if (attributes == null || attributes.isEmpty()) {
                System.out.println("No attributes within the notified entity, nothing is done (id=" + entityId
                        + ", type=" + entityType + ")");
                return;
            } // if

            String line = recvTime + csvSeparator + servicePath + csvSeparator + entityId + csvSeparator + entityType;

            for (Attributes attribute : attributes) {
                String attrName = attribute.getAttrName();
                String attrType = attribute.getAttrType();
                String attrValue = attribute.getAttrValue();
                String attrMetadata = attribute.getMetadataString();
                System.out.println("[Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");

                // this has to be done notification by notification and not at initialization since in row mode not all
                // the notifications contain all the attributes
                String attrMdFileName = buildAttrMdFilePath(service, servicePath, destination, attrName, attrType);
                String printableAttrMdFileName = "hdfs:///user/" + username + "/" + attrMdFileName;
                String mdAggregation = mdAggregations.get(attrMdFileName);

                if (mdAggregation == null) {
                    mdAggregation = new String();
                } // if

                // agregate the metadata
                String concatMdAggregation;

                if (mdAggregation.isEmpty()) {
                    concatMdAggregation = getCSVMetadata(attrMetadata, recvTimeTs);
                } else {
                    concatMdAggregation = mdAggregation.concat("\n" + getCSVMetadata(attrMetadata, recvTimeTs));
                } // if else

                mdAggregations.put(attrMdFileName, concatMdAggregation);

                // create part of the line with the current attribute (a.k.a. a column)
                line += csvSeparator + attrValue.replaceAll("\"", "") + csvSeparator + printableAttrMdFileName;
            } // for

            // now, aggregate the line
            if (aggregation.isEmpty()) {
                aggregation = line;
            } else {
                aggregation += "\n" + line;
            } // if else
        } // aggregate

        private String getCSVMetadata(String attrMetadata, long recvTimeTs) throws Exception {
            String csvMd = "";

            // metadata is in JSON format, decode it
            JSONParser jsonParser = new JSONParser();
            JSONArray attrMetadataJSON = (JSONArray) jsonParser.parse(attrMetadata);

            // iterate on the metadata
            for (Object mdObject : attrMetadataJSON) {
                JSONObject mdJSONObject = (JSONObject) mdObject;
                String line = recvTimeTs + ","
                        + mdJSONObject.get("name") + ","
                        + mdJSONObject.get("type") + ","
                        + mdJSONObject.get("value");

                if (csvMd.isEmpty()) {
                    csvMd = line;
                } else {
                    csvMd += "\n" + line;
                } // if else
            } // for

            return csvMd;
        } // getCSVMetadata

    } // CSVColumnAggregator

    public HDFSAggregator getAggregator(String fileFormat) {
        switch (fileFormat) {
            case "json-row":
                return new JSONRowAggregator();
            case "json-column":
                return new JSONColumnAggregator();
            case "cvs-row":
                return new CSVRowAggregator();
            case "cvs-column":
                return new CSVColumnAggregator();
            default:
                return null;
        } // switch
    } // getAggregator

    public void persistAggregation(HDFSAggregator aggregator,boolean enableLowercase, HDFSBackend persistenceBackend) throws Exception {
        String aggregation = aggregator.getAggregation();
        String hdfsFolder = aggregator.getFolder(enableLowercase);
        String hdfsFile = aggregator.getFile(enableLowercase);

        System.out.println("[ Persisting data at OrionHDFSSink. HDFS file ("
                + hdfsFile + "), Data (" + aggregation + ")");

        if (persistenceBackend.exists(hdfsFile)) {
            persistenceBackend.append(hdfsFile, aggregation);
        } else {
            persistenceBackend.createDir(hdfsFolder);
            persistenceBackend.createFile(hdfsFile, aggregation);
        } // if else
    } // persistAggregation

    public void persistMDAggregations(HDFSAggregator aggregator, HDFSBackend persistenceBackend) throws Exception {
        Set<String> attrMDFiles = aggregator.getAggregatedAttrMDFiles();

        for (String hdfsMDFile : attrMDFiles) {
            String hdfsMdFolder = hdfsMDFile.substring(0, hdfsMDFile.lastIndexOf("/"));
            String mdAggregation = aggregator.getMDAggregation(hdfsMDFile);

            System.out.println("[Persisting metadata at OrionHDFSSink. HDFS file ("
                    + hdfsMDFile + "), Data (" + mdAggregation + ")");

            if (persistenceBackend.exists(hdfsMDFile)) {
                persistenceBackend.append(hdfsMDFile, mdAggregation);
            } else {
                persistenceBackend.createDir(hdfsMdFolder);
                persistenceBackend.createFile(hdfsMDFile, mdAggregation);
            } // if else
        } // for
    } // persistMDAggregations

    public void provisionHiveTable(HDFSAggregator aggregator, String dbName,boolean enableLowercase, String fileFormat, boolean serviceAsNamespace, String hiveHost,String hivePort, String hiveServerVersion, String username, String password) throws Exception {
        String dirPath = aggregator.getFolder(enableLowercase);
        String fields = aggregator.getHiveFields();
        String tag;

        switch (fileFormat) {
            case "json-row":
            case "cvs-row":
                tag = "_row";
                break;
            case "json-column":
            case "cvs-column":
                tag = "_column";
                break;
            default:
                tag = "";
        } // switch

        // get the table name to be created
        // the replacement is necessary because Hive, due it is similar to MySQL, does not accept '-' in the table names
        String tableName = NGSICharsets.encodeHive((serviceAsNamespace ? "" : username + "_") + dirPath) + tag;
        System.out.println("Creating Hive external table '" + tableName + "' in database '"  + dbName + "'");

        // get a Hive client
        HiveBackend hiveClient = new HiveBackend(hiveServerVersion, hiveHost, hivePort, username, password);

        // create the query
        String query;

        switch (fileFormat) {
            case "json-column":
            case "json-row":
                query = "create external table if not exists " + dbName + "." + tableName + " (" + fields
                        + ") row format serde " + "'org.openx.data.jsonserde.JsonSerDe' with serdeproperties "
                        + "(\"dots.in.keys\" = \"true\") location '/user/"
                        + (serviceAsNamespace ? "" : (username + "/")) + dirPath + "'";
                break;
            case "cvs-column":
            case "cvs-row":
                query = "create external table if not exists " + dbName + "." + tableName + " (" + fields
                        + ") row format " + "delimited fields terminated by ',' location '/user/"
                        + (serviceAsNamespace ? "" : (username + "/")) + dirPath + "'";
                break;
            default:
                query = "";
        } // switch

        // execute the query
        System.out.println("Doing Hive query: '" + query + "'");

        if (!hiveClient.doCreateTable(query)) {
            System.out.println("The HiveQL external table could not be created, but Draco can continue working... "
                    + "Check your Hive/Shark installation");
        } // if
    } // provisionHive
    /*
    /**
     * Builds a HDFS folder path.
     * @param service
     * @param servicePath
     * @param destination
     * @return The HDFS folder path
     */
    public String buildFolderPath(String service, String servicePath, String destination, boolean enableEncoding) throws Exception {
        String folderPath;

        if (enableEncoding) {
            folderPath = NGSICharsets.encodeHDFS(service, false) + NGSICharsets.encodeHDFS(servicePath, true)
                    + (servicePath.equals("/") ? "" : "/") + NGSICharsets.encodeHDFS(destination, false);
        } else {
            folderPath = NGSICharsets.encode(service, false, true) + NGSICharsets.encode(servicePath, false, false)
                    + (servicePath.equals("/") ? "" : "/") + NGSICharsets.encode(destination, false, true);
        } // if else

        if (folderPath.length() > NGSIConstants.HDFS_MAX_NAME_LEN) {
            throw new Exception("Building folder path name '" + folderPath + "' and its length is "
                    + "greater than " + NGSIConstants.HDFS_MAX_NAME_LEN);
        } // if

        return folderPath;
    } // buildFolderPath

    /**
     * Builds a HDFS file path.
     * @param service
     * @param servicePath
     * @param destination
     * @return The file path
     */
    public String buildFilePath(String service, String servicePath, String destination, boolean enableEncoding) throws Exception {
        String filePath;

        if (enableEncoding) {
            filePath = NGSICharsets.encodeHDFS(service, false) + NGSICharsets.encodeHDFS(servicePath, true)
                    + (servicePath.equals("/") ? "" : "/") + NGSICharsets.encodeHDFS(destination, false)
                    + "/" + NGSICharsets.encodeHDFS(destination, false) + ".txt";
        } else {
            filePath = NGSICharsets.encode(service, false, true) + NGSICharsets.encode(servicePath, false, false)
                    + (servicePath.equals("/") ? "" : "/") + NGSICharsets.encode(destination, false, true)
                    + "/" + NGSICharsets.encode(destination, false, true) + ".txt";
        } // if else

        if (filePath.length() > NGSIConstants.HDFS_MAX_NAME_LEN) {
            throw new Exception("Building file path name '" + filePath + "' and its length is "
                    + "greater than " + NGSIConstants.HDFS_MAX_NAME_LEN);
        } // if

        return filePath;
    } // buildFilePath

    /**
     * Builds an attribute metadata HDFS folder path.
     * @param service
     * @param servicePath
     * @param destination
     * @param attrName
     * @param attrType
     * @return The attribute metadata HDFS folder path
     */
    public String buildAttrMdFolderPath(String service, String servicePath, String destination, String attrName,
                                           String attrType) {
        return NGSICharsets.encodeHDFS(service, false) + NGSICharsets.encodeHDFS(servicePath, true)
                + (servicePath.equals("/") ? "" : "/")
                + NGSICharsets.encodeHDFS(destination, false) + CommonConstants.CONCATENATOR
                + NGSICharsets.encodeHDFS(attrName, false) + CommonConstants.CONCATENATOR
                + NGSICharsets.encodeHDFS(attrType, false);
    } // buildAttrMdFolderPath

    /**
     * Builds an attribute metadata HDFS file path.
     * @param service
     * @param servicePath
     * @param destination
     * @param attrName
     * @param attrType
     * @return The attribute metadata HDFS file path
     */
    public String buildAttrMdFilePath(String service, String servicePath, String destination, String attrName,
                                         String attrType) {
        return NGSICharsets.encodeHDFS(service, false) + NGSICharsets.encodeHDFS(servicePath, true)
                + (servicePath.equals("/") ? "" : "/")
                + NGSICharsets.encodeHDFS(destination, false) + CommonConstants.CONCATENATOR
                + NGSICharsets.encodeHDFS(attrName, false) + CommonConstants.CONCATENATOR
                + NGSICharsets.encodeHDFS(attrType, false) + "/"
                + NGSICharsets.encodeHDFS(destination, false) + CommonConstants.CONCATENATOR
                + NGSICharsets.encodeHDFS(attrName, false) + CommonConstants.CONCATENATOR
                + NGSICharsets.encodeHDFS(attrType, false) + ".txt";
    } // buildAttrMdFolderPath
}
