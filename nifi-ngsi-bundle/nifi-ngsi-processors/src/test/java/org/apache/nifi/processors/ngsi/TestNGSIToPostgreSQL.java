package org.apache.nifi.processors.ngsi;

import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.ngsi.NGSI.backends.PostgreSQLBackend;
import org.apache.nifi.processors.ngsi.NGSI.utils.Entity;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.util.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class TestNGSIToPostgreSQL {
    TestRunner runner;
    PostgreSQLBackend backend;


    @Before
    public void setUp() throws Exception {
        //Mock the DBCP Controller Service so we can control the Results

        final Map<String, String> dbcpProperties = new HashMap<>();

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
            String builtSchemaName = backend.buildSchemaName(service,enableEncoding,enableLowercase);
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
            String builtSchemaName = backend.buildSchemaName(service,enableEncoding,enableLowercase);
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
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();

        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);


        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
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
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
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
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
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
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
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
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);

        try {
            backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - The root service path was not detected as not valid");
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "-  OK  - The root service path was detected as not valid");
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
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);
        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
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
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
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
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
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
            backend.buildSchemaName(service,enableEncoding,enableLowercase);
            System.out.println("[NGSIToPostgreSQL.buildSchemaName]"
                    + "- FAIL - A schema name length greater than 63 characters has not been detected");
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
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
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooongServicePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            backend.buildTableName(servicePath,entity,dataModel,enableEncoding,enableLowercase);
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - A table name length greater than 63 characters has not been detected");
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
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
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooongServicePath";
        Entity entity = new Entity("tooLooooooooooooooooooooooooooongEntity", "someType",null);


        try {
            backend.buildTableName(servicePath,entity,dataModel,enableEncoding,enableLowercase);
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - A table name length greater than 63 characters has not been detected");
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
            System.out.println("[NGSIToPostgreSQL.buildTableName]"
                    + "-  OK  - A table name length greater than 63 characters has been detected");
        } // try catch
    } // testBuildTableNameLengthDataModelByEntity



}
