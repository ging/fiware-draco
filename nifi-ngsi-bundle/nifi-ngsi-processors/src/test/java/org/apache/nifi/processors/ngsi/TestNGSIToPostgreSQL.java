package org.apache.nifi.processors.ngsi;

import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.ngsi.ngsi.backends.PostgreSQLBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Attributes;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
            List<String> valuesForInsert = backend.getValuesForInsert(attrPersistence, entity, Collections.emptyMap(), creationTime, fiwareServicePath,ngsiVersion,false,false,"");
            List<String> expecetedvaluesForInsert = List.of("('1562561734983','07/08/2019 04:55:34','','someId','someType','someAttr','someType','someValue','[]')");
           
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
            List<String> valuesForInsert = backend.getValuesForInsert(attrPersistence, entity, Collections.emptyMap(), creationTime, fiwareServicePath,ngsiVersion,false,false,"");
            List<String> expecetedvaluesForInsert = List.of("('1562561734983','07/08/2019 04:55:34','','someId','someType','someValue','[]')");
           
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
            List<String> valuesForInsert = backend.getValuesForInsert(attrPersistence, entity, Collections.emptyMap(), creationTime, fiwareServicePath,ngsiVersion,false,false,"");
            List<String> expecetedvaluesForInsert = List.of("('1562561734983','07/08/2019 04:55:34','','someId','someType','someAttr','someType','someValue','someMtdStr')");
           
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
            List<String> valuesForInsert = backend.getValuesForInsert(attrPersistence, entity, Collections.emptyMap(), creationTime, fiwareServicePath,ngsiVersion,false,false, "");
            List<String> expecetedvaluesForInsert = List.of("('1562561734983','07/08/2019 04:55:34','','someId','someType','someValue','someMtdStr')");
           
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
        String expectedValuesForInsert = "(62,'2021-02-19T13:18:19.000000Z',null,null,'2019-07-08T04:55:34.983Z','urn:ngsi-ld:AgriCropRecord:gael:ble:3d4bc657-744f-43d7-b741-b5fd6e9a1449',72,'2021-02-19T13:18:19.000000Z',1024,'2021-02-19T13:18:19.000000Z',14.6,'2021-02-19T13:18:19.000000Z','AgriCropRecord',25.2,null,null,'2021-02-19T09:13:15.000000Z',null,null)";
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

        String schemaName = backend.buildSchemaName("test", true, false, false);
        String tableName = backend.buildTableName("", entities.get(0), "db-by-entity-type", true, false, NGSI_LD_VERSION, false);

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
        assertTrue(instertQueryValue.split("values")[1].split("\\(").length == 5);
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
