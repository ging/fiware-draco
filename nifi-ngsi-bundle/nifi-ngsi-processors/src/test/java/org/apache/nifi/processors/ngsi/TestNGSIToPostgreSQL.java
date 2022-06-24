package org.apache.nifi.processors.ngsi;

import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.ngsi.ngsi.backends.PostgreSQLBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Attributes;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import org.apache.nifi.processors.ngsi.ngsi.utils.Metadata;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIConstants.POSTGRESQL_COLUMN_TYPES;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.json.JSONArray;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class TestNGSIToPostgreSQL {
    private TestRunner runner;
    private PostgreSQLBackend backend;

    private NGSIUtils ngsiUtils = new NGSIUtils();

    private InputStream inputStream = getClass().getClassLoader().getResourceAsStream("temporalEntity.json");

    private String NGSI_LD_VERSION = "ld";

    @Before
    public void setUp() throws Exception {
        //Mock the DBCP Controller Service so we can control the Results
        runner = TestRunners.newTestRunner(NGSIToMySQL.class);
        runner.setProperty(NGSIToPostgreSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(NGSIToPostgreSQL.NGSI_VERSION, "v2");
        runner.setProperty(NGSIToPostgreSQL.DATA_MODEL, "db-by-service-path");
        runner.setProperty(NGSIToPostgreSQL.ATTR_PERSISTENCE, "row");
        runner.setProperty(NGSIToPostgreSQL.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToPostgreSQL.ENABLE_LOWERCASE, "false");
        runner.setProperty(NGSIToPostgreSQL.BATCH_SIZE, "100");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "false");
        backend = new PostgreSQLBackend();
    }

    /**
     * [NGSIToPostgreSQL.buildDBName] -------- The schema name is equals to the encoding of the notified/defaulted
     * service.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildDBNameOldEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildDBName]"
                + "-------- The schema name is equals to the encoding of the notified/defaulted service");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean(); // default
        String service = "someService";

        try {
            String builtSchemaName = backend.buildSchemaName(service,enableEncoding,enableLowercase,false);
            String expectedDBName = "someService";

            try {
                assertEquals(expectedDBName, builtSchemaName);
                System.out.println("[NGSIToPostgreSQL.buildDBName]"
                        + "-  OK  - '" + expectedDBName + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildDBName]"
                        + "- FAIL - '" + expectedDBName + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildDBName]"
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildDBNameOldEncoding

    /**
     * [NGSIToPostgreSQL.buildDBName] -------- The schema name is equals to the encoding of the notified/defaulted
     * service.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildDBNameNewEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildDBName]"
                + "-------- The schema name is equals to the encoding of the notified/defaulted service");

        String service = "someService";
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();


        try {
            String builtSchemaName = backend.buildSchemaName(service,enableEncoding,enableLowercase,false);
            String expectedDBName = "somex0053ervice";

            try {
                assertEquals(expectedDBName, builtSchemaName);
                System.out.println("[NGSIToPostgreSQL.buildDBName]"
                        + "-  OK  - '" + expectedDBName + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildDBName]"
                        + "- FAIL - '" + expectedDBName + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildDBName]"
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildDBNameNewEncoding

    /**
     * [NGSIToPostgreSQL.buildTableName] -------- When a non root service-path is notified/defaulted and
     * data_model is 'dm-by-service-path' the MySQL table name is the encoding of <service-path>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameNonRootServicePathDataModelByServicePathOldEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();

        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);


        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
            String expecetedTableName = "somePath";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByServicePathOldEncoding

    /**
     * [NGSIToPostgreSQL.buildTableName] -------- When a non root service-path is notified/defaulted and
     * data_model is 'dm-by-service-path' the MySQL table name is the encoding of <service-path>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameNonRootServicePathDataModelByServicePathNewEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of <service-path>");

runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
            String expecetedTableName = "x002fsomex0050ath";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByServicePathNewEncoding

    /**
     * [NGSIToPostgreSQL.buildTableName] -------- When a non root service-path is notified/defaulted and
     * data_model is 'dm-by-entity' the MySQL table name is the encoding of the concatenation of \<service-path\>,
     * \<entity_id\> and \<entity_type\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameNonRootServicePathDataModelByEntityOldEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");
        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
            String expecetedTableName = "somePath_someId_someType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByEntityOldEncoding

    /**
     * [NGSIToPostgreSQL.buildTableName] -------- When a non root service-path is notified/defaulted and
     * data_model is 'dm-by-entity' the MySQL table name is the encoding of the concatenation of \<service-path\>,
     * \<entity_id\> and \<entity_type\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameNonRootServicePathDataModelByEntityNewEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");
        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
            String expecetedTableName = "x002fsomex0050athxffffsomex0049dxffffsomex0054ype";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByEntityNewEncoding

    /**
     * [NGSIToPostgreSQL.buildTableName] -------- When a root service-path is notified/defaulted and
     * data_model is 'dm-by-service-path' the MySQL table name cannot be built.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameRootServicePathDataModelByServicePathOldEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When a root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name cannot be built");

        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-service-path");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);
        String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
        String expecetedTableName = "";

        try {

            assertEquals(expecetedTableName,builtTableName);
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "-  OK  - The root service path was detected as not valid");

        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - The root service path was not detected as not valid");

        } // try catch
    } // testBuildTableNameRootServicePathDataModelByServicePathOldEncoding

    /**
     * [NGSIToPostgreSQL.buildTableName] -------- When a root service-path is notified/defaulted and
     * data_model is 'dm-by-service-path' the MySQL table name is the encoding of \<service-path\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameRootServicePathDataModelByServicePathNewEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When a root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);
        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
            String expecetedTableName = "x002f";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByServicePathNewEncoding

    /**
     * [NGSIToPostgreSQL.buildTableName] -------- When a root service-path is notified/defaulted and
     * data_model is 'dm-by-entity' the MySQL table name is the encoding of the concatenation of \<service-path\>,
     * \<entityId\> and \<entityType\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameRootServicePathDataModelByEntityOldEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When a root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
            String expecetedTableName = "someId_someType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByEntityOldencoding

    /**
     * [NGSIToPostgreSQL.buildTableName] -------- When a root service-path is notified/defaulted and
     * data_model is 'dm-by-entity' the MySQL table name is the encoding of the concatenation of \<service-path\>,
     * \<entityId\> and \<entityType\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameRootServicePathDataModelByEntityNewEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When a root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
            String expecetedTableName = "x002fxffffsomex0049dxffffsomex0054ype";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByEntityNewEncoding

    /**
     * [NGSIToPostgreSQL.buildSchemaName] -------- A schema name length greater than 63 characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildSchemaNameLength() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildSchemaName]"
                + "-------- A schema name length greater than 63 characters is detected");

        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String service = "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongService";

        try {
            backend.buildSchemaName(service,enableEncoding,enableLowercase,false);
            fail("[NGSIToPostgreSQL.buildSchemaName]"
                    + "- FAIL - A schema name length greater than 63 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildSchemaName]"
                    + "-  OK  - A schema name length greater than 63 characters has been detected");
        } // try catch
    } // testBuildSchemaNameLength

    /**
     * [NGSIToPostgreSQL.buildTableName] -------- When data model is by service path, a table name length greater
     * than 63 characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameLengthDataModelByServicePath() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When data model is by service path, a table name length greater than 63 characters is "
                + "detected");

        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-service-path");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooongServicePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            backend.buildTableName(servicePath,entity,dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
            fail("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - A table name length greater than 63 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "-  OK  - A table name length greater than 63 characters has been detected");
        } // try catch
    } // testBuildTableNameLengthDataModelByServicePath

    /**
     * [NGSICartoDBSink.buildTableName] -------- When data model is by entity, a table name length greater than 63
     * characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildTableNameLengthDataModelByEntity() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When data model is by entity, a table name length greater than 63 characters is detected");

        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooongServicePath";
        Entity entity = new Entity("tooLooooooooooooooooooooooooooongEntity", "someType",null);


        try {
            backend.buildTableName(servicePath,entity,dataModel,enableEncoding,enableLowercase,ngsiVersion,false);
            fail("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - A table name length greater than 63 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "-  OK  - A table name length greater than 63 characters has been detected");
        } // try catch
    } // testBuildTableNameLengthDataModelByEntity
    
    @Test
    public void testRowFields() throws Exception {
        System.out.println("[PostgreSQLBackend.listOfFields ]"
                + "-------- When attrPersistence is column");

        runner.setProperty(NGSIToMySQL.ATTR_PERSISTENCE, "row");
        String attrPersistence = runner.getProcessContext().getProperty(NGSIToMySQL.ATTR_PERSISTENCE).getValue();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();
        
        ArrayList<Attributes> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attributes("someAttr", "someType", "SomeValue", null, null));
        Entity entity = new Entity("someId", "someType", entityAttrs);
        
        try {
            Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields = backend.listOfFields(attrPersistence, entity,ngsiVersion,false, false, "");
            List<String> expList = Arrays.asList("recvTimeTs", "recvTime", "fiwareServicePath", "entityId", "entityType", "attrName", "attrType", "attrValue", "attrMd");
            Set<String> expecetedListOfFields = new HashSet<>(expList);
           
            try {
                assertEquals(expecetedListOfFields, listOfFields.keySet());
                System.out.println("[PostgreSQLBackend.listOfFields]"
                        + "-  OK  - '" + listOfFields + "' is equals to the expected output");
            } catch (AssertionError e) {
                System.out.println("[PostgreSQLBackend.listOfFields]"
                        + "- FAIL - '" + listOfFields + "' is not equals to the expected output");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[PostgreSQLBackend.listOfFields]"
                    + "- FAIL - There was some problem when building the list of fields");
            throw e;
        } // try catch

    } // testRowFields

    @Test
    public void testColumnFields() throws Exception {
        System.out.println("[PostgreSQLBackend.listOfFields ]"
                + "-------- When attrPersistence is column");

        runner.setProperty(NGSIToMySQL.ATTR_PERSISTENCE, "column");
        String attrPersistence = runner.getProcessContext().getProperty(NGSIToMySQL.ATTR_PERSISTENCE).getValue();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();

        ArrayList<Attributes> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attributes("someAttr", "someType", "SomeValue", null, null));
        Entity entity = new Entity("someId", "someType", entityAttrs);
        
        try {
            Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields = backend.listOfFields(attrPersistence, entity,ngsiVersion,false, false, "");
            List<String> expList = Arrays.asList("recvTimeTs", "recvTime", "fiwareServicePath", "entityId", "entityType", "someAttr", "someAttr_md");
            Set<String> expecetedListOfFields = new HashSet<>(expList);
           
            try {
                assertEquals(expecetedListOfFields, listOfFields.keySet());
                System.out.println("[PostgreSQLBackend.listOfFields]"
                        + "-  OK  - '" + listOfFields + "' is equals to the expected output");
            } catch (AssertionError e) {
                System.out.println("[PostgreSQLBackend.listOfFields]"
                        + "- FAIL - '" + listOfFields + "' is not equals to the expected output");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[PostgreSQLBackend.listOfFields]"
                    + "- FAIL - There was some problem when building the list of fields");
            throw e;
        } // try catch

    } // testColumnFields
    
    @Test
    public void testValuesForInsertRowWithoutMetada() throws Exception {
        System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                + "-------- When attrPersistence is column");

        runner.setProperty(NGSIToMySQL.ATTR_PERSISTENCE, "row");
        String attrPersistence = runner.getProcessContext().getProperty(NGSIToMySQL.ATTR_PERSISTENCE).getValue();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();


        ArrayList<Attributes> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attributes("someAttr", "someType", "someValue", null, null));
        Entity entity = new Entity("someId", "someType", entityAttrs);
        long creationTime = 1562561734983l;
        String fiwareServicePath = "/";
        
        try {
            String valuesForInsert = backend.getValuesForInsert(attrPersistence, entity, Collections.emptyMap(), creationTime, fiwareServicePath,ngsiVersion,false, false, "").get(0);
            String expecetedvaluesForInsert = "('1562561734983','07/08/2019 04:55:34','','someId','someType','someAttr','someType','someValue','[]')";
           
            try {
                assertEquals(expecetedvaluesForInsert, valuesForInsert);
                System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                        + "-  OK  - '" + valuesForInsert + "' is equals to the expected output");
            } catch (AssertionError e) {
                System.out.println("[PostgreSQLBackend.valuesForInsert]"
                        + "- FAIL - '" + valuesForInsert + "' is not equals to the expected output");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[PostgreSQLBackend.valuesForInsert]"
                    + "- FAIL - There was some problem when building values for insert");
            throw e;
        } // try catch

    } // testValuesForInsertRowWithoutMetada
    
    @Test
    public void testValuesForInsertColumnWithoutMetadata() throws Exception {
        System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                + "-------- When attrPersistence is column");

        runner.setProperty(NGSIToMySQL.ATTR_PERSISTENCE, "column");
        String attrPersistence = runner.getProcessContext().getProperty(NGSIToMySQL.ATTR_PERSISTENCE).getValue();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();

        ArrayList<Attributes> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attributes("someAttr", "someType", "someValue", null, null));
        Entity entity = new Entity("someId", "someType", entityAttrs);
        long creationTime = 1562561734983l;
        String fiwareServicePath = "/";
        
        try {
            String valuesForInsert = backend.getValuesForInsert(attrPersistence, entity, Collections.emptyMap(), creationTime, fiwareServicePath,ngsiVersion,false, false, "").get(0);
            String expecetedvaluesForInsert = "('1562561734983','07/08/2019 04:55:34','','someId','someType','someValue','[]')";
           
            try {
                assertEquals(expecetedvaluesForInsert, valuesForInsert);
                System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                        + "-  OK  - '" + valuesForInsert + "' is equals to the expected output");
            } catch (AssertionError e) {
                System.out.println("[PostgreSQLBackend.valuesForInsert]"
                        + "- FAIL - '" + valuesForInsert + "' is not equals to the expected output");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[PostgreSQLBackend.valuesForInsert]"
                    + "- FAIL - There was some problem when building values for insert");
            throw e;
        } // try catch

    } // testValuesForInsertColumnWithoutMetadata
    
    @Test
    public void testValuesForInsertRowWithMetadata() throws Exception {
        System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                + "-------- When attrPersistence is column");

        runner.setProperty(NGSIToMySQL.ATTR_PERSISTENCE, "row");
        String attrPersistence = runner.getProcessContext().getProperty(NGSIToMySQL.ATTR_PERSISTENCE).getValue();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();


        ArrayList<Metadata> attrMetadata = new ArrayList<>();
        attrMetadata.add(new Metadata("mtdName", "mtdType", "mtdValue"));
        ArrayList<Attributes> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attributes("someAttr", "someType", "someValue", attrMetadata, "someMtdStr"));
        Entity entity = new Entity("someId", "someType", entityAttrs);
        long creationTime = 1562561734983l;
        String fiwareServicePath = "/";
        
        try {
            String valuesForInsert = backend.getValuesForInsert(attrPersistence, entity, Collections.emptyMap(), creationTime, fiwareServicePath,ngsiVersion,false, false, "").get(0);
            String expecetedvaluesForInsert = "('1562561734983','07/08/2019 04:55:34','','someId','someType','someAttr','someType','someValue','someMtdStr')";
           
            try {
                assertEquals(expecetedvaluesForInsert, valuesForInsert);
                System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                        + "-  OK  - '" + valuesForInsert + "' is equals to the expected output");
            } catch (AssertionError e) {
                System.out.println("[PostgreSQLBackend.valuesForInsert]"
                        + "- FAIL - '" + valuesForInsert + "' is not equals to the expected output");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[PostgreSQLBackend.valuesForInsert]"
                    + "- FAIL - There was some problem when building values for insert");
            throw e;
        } // try catch

    } // testValuesForInsertRowWithMetadata
    
    @Test
    public void testValuesForInsertColumnWithMetadata() throws Exception {
        System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                + "-------- When attrPersistence is column");

        runner.setProperty(NGSIToMySQL.ATTR_PERSISTENCE, "column");
        String attrPersistence = runner.getProcessContext().getProperty(NGSIToMySQL.ATTR_PERSISTENCE).getValue();
        String ngsiVersion = runner.getProcessContext().getProperty(NGSIToMySQL.NGSI_VERSION).getValue();


        ArrayList<Metadata> attrMetadata = new ArrayList<>();
        attrMetadata.add(new Metadata("mtdName", "mtdType", "mtdValue"));
        ArrayList<Attributes> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attributes("someAttr", "someType", "someValue", attrMetadata, "someMtdStr"));
        Entity entity = new Entity("someId", "someType", entityAttrs);
        long creationTime = 1562561734983l;
        String fiwareServicePath = "/";
        
        try {
            String valuesForInsert = backend.getValuesForInsert(attrPersistence, entity, Collections.emptyMap(), creationTime, fiwareServicePath,ngsiVersion,false, false, "").get(0);
            String expecetedvaluesForInsert = "('1562561734983','07/08/2019 04:55:34','','someId','someType','someValue','someMtdStr')";
           
            try {
                assertEquals(expecetedvaluesForInsert, valuesForInsert);
                System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                        + "-  OK  - '" + valuesForInsert + "' is equals to the expected output");
            } catch (AssertionError e) {
                System.out.println("[PostgreSQLBackend.valuesForInsert]"
                        + "- FAIL - '" + valuesForInsert + "' is not equals to the expected output");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[PostgreSQLBackend.valuesForInsert]"
                    + "- FAIL - There was some problem when building values for insert");
            throw e;
        } // try catch

    } // testValuesForInsertColumnWithMetadata

    @Test
    public void testValuesForInsertColumnForNgsiLd() throws IOException {
        runner.setProperty(NGSIToMySQL.ATTR_PERSISTENCE, "column");
        String attrPersistence = runner.getProcessContext().getProperty(NGSIToMySQL.ATTR_PERSISTENCE).getValue();

        String data = readFromInputStream(inputStream);
        ArrayList<Entity> entities = ngsiUtils.parseNgsiLdEntities(new JSONArray(data));

        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields = backend.listOfFields(
                attrPersistence,
                entities.get(0),
                NGSI_LD_VERSION,
                false,
                true,
                ""
        );

        long creationTime = 1562561734983l;

        TimeZone.setDefault(TimeZone.getTimeZone("CEST"));
        ZonedDateTime creationDate = Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC);

        List<String> timeStamps = entities.get(0).getEntityAttrsLD().stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt)).keySet().stream().collect(Collectors.toList());
        String expectedValuesForInsert = "(0.3150114-0.27014965-0.26770955-0.25650775-0.25381634-0.25117627-0.2565934-0.26185906-0.26760414-0.26761946-0.26776528-0.26734355-0.2642796-0.2650225-0.25940326-0.26099572-0.2593427-0.26269966-0.26579183-0.2671962-0.27016798-0.27163747-0.27148527-0.27168998-0.27359295-0.27290982-0.2738807-0.2732242-0.27322257-0.27081263-0.2711597-0.27033675-0.26809087-0.2680587-0.26747066-0.26921746-0.26680875-0.26623058-0.2652376-0.26782638-0.26519278-0.26502314-0.2642374-0.26454332-0.26245463-0.2625531-0.26188976-0.26024806-0.2593902-0.2609253-0.26031616-0.26048815-0.25958866-0.26034683-0.26083097-0.26012954-0.25934565-0.2589371-0.2595384-0.2599644-0.258589-0.25807604-0.2591431-0.2583716-0.26128557-0.25718522-0.2593607-0.25739777-0.2588574-0.25812826-0.2584352-0.2595222-0.25914037-0.2608788-0.26025066-0.26239604-0.26276523-0.26278302-0.26278836-0.26228207-0.26296666-0.26406553-0.2663875-0.26589608-0.26712307-0.2679805-0.2679655-0.2697938-0.27164736-0.27197325-0.2729412-0.27358106-0.2738609-0.27317166-0.27270672-0.27089566-0.26856464-0.26971993-0.2686933-0.2682729-0.26803994-0.26755965-0.26740283-0.26728353-0.26710823-0.26780194-0.2670447-0.26799357-0.2693484-0.26871884-0.26976213-0.26991397-0.27004978-0.2700963-0.27010328-0.2693086-0.27038074-0.2693631-0.2687514-0.26855028-0.2685341-0.26709747-0.26640835-0.26779395-0.2668755-0.2665464-0.2659598-0.2664415-0.26632422-0.26685834-0.26668373-0.26686156-0.2660256-0.26753837-0.267832-0.2685235-0.26973164-0.26963788-0.2714157-0.2718171-0.27080035-0.271392-0.2723557-0.2722909-0.27329233-0.27616194-0.2784247-0.28289562-0.2852722-0.28743747-0.28794998-0.28848863-0.28951636-0.29106066-0.29631457-0.2995764-0.30122408-0.30157167-0.30043384-0.30002218-0.30022523-0.3016346-0.30228823-0.30281088-0.30440927-0.30484238-0.30593693-0.30641472-0.3074286-0.3068012-0.30800223-0.3074318-0.30865747-0.30849558-0.30865642-0.30899888-0.30847687-0.30830628-0.30865642-0.30783242-0.3074537-0.3060785-0.30694702-0.30714974-0.30625597-0.30499545-0.30480498-0.30332473-0.30316105-0.30156085-0.3015956-0.30124947-0.3014249-0.3024441-0.30261394-0.3029554-0.30264723-0.30334944-0.3039961-0.30343145-0.30238158-0.30403265-0.30307344-0.3040367-0.30483016-0.30425265-0.30482206-0.30459952-0.3063734-0.30353388-0.30496177-0.30333513-0.30578002-0.3056758-0.30657715-0.3059596-0.30516008-0.30717614-0.30672467-0.304655-0.30595672-0.30535197-0.30543545-0.3033213-0.30383888-0.30465493-0.30232707-0.3023196-0.30030468-0.29666194-0.29869685-0.29931054-0.2988543-0.29899678-0.3000596-0.2999892-0.30143398-0.30111724-0.29966423-0.3041462-0.3008776-0.2939069-0.28281817-0.26714736-0.24725205-0.23738693-0.22203125-0.20480478-0.19340736-0.177902-0.1828181-0.18810563-0.19585791-0.21631233-0.21040721-0.21932182-,'2021-02-19T13:18:19.000000Z','urn:ngsi-ld:AgriCropRecord:gael:ble:3d4bc657-744f-43d7-b741-b5fd6e9a1449','AgriCropRecord',72,'2021-02-19T13:18:19.000000Z',1024,'2021-02-19T13:18:19.000000Z',14.6,'2021-02-19T13:18:19.000000Z',25.2,'2021-02-19T09:13:15.000000Z','2019-07-08T04:55:34.983Z')";

        List<String> valuesForInsert = backend.getValuesForInsert(
                attrPersistence,
                entities.get(0),
                listOfFields,
                creationTime,
                "",
                NGSI_LD_VERSION,
                false,
                true,
                ""
        );
        assertEquals(4, valuesForInsert.size());
        assertEquals(expectedValuesForInsert, valuesForInsert.get(1));
        for (int i = 0; i < timeStamps.size(); i++) {
            assertTrue(valuesForInsert.get(i).contains(timeStamps.get(i)));
            assertTrue(valuesForInsert.get(i).contains(DateTimeFormatter.ISO_INSTANT.format(creationDate)));
        }
    }

    @Test
    public void testInsertQueryForNgsiLd() throws Exception {
        runner.setProperty(NGSIToMySQL.ATTR_PERSISTENCE, "column");
        String attrPersistence = runner.getProcessContext().getProperty(NGSIToMySQL.ATTR_PERSISTENCE).getValue();

        String data = readFromInputStream(inputStream);
        ArrayList<Entity> entities = ngsiUtils.parseNgsiLdEntities(new JSONArray(data));

        String schemaName = backend.buildSchemaName("test",true,false, false);
        String tableName = backend.buildTableName("", entities.get(0),"db-by-entity-type",true, false, NGSI_LD_VERSION, false);

        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields = backend.listOfFields(
                attrPersistence,
                entities.get(0),
                NGSI_LD_VERSION,
                false,
                true,
                ""
        );

        long creationTime = 1562561734983l;

        String instertQueryValue = backend.insertQuery(
                entities.get(0),
                creationTime,
                "",
                schemaName,
                tableName,
                listOfFields,
                "db-by-entity-type",
                NGSI_LD_VERSION,
                false,
                true,
                ""
        );
        assertTrue(instertQueryValue.split("values")[1].split("\\(").length ==5);
    }

    private String readFromInputStream(InputStream inputStream)
            throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }
}
