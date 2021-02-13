package org.apache.nifi.processors.ngsi;

import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.ngsi.ngsi.backends.MySQLBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.*;


@RunWith(JUnit4.class)
public class TestNGSIToMySQL {

   private TestRunner runner;
   private MySQLBackend backend;

    @Before
   public void setUp() throws Exception {

        runner = TestRunners.newTestRunner(NGSIToMySQL.class);
        runner.setProperty(NGSIToMySQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(NGSIToMySQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(NGSIToMySQL.NGSI_VERSION, "v2");
        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-service-path");
        runner.setProperty(NGSIToMySQL.ATTR_PERSISTENCE, "row");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToMySQL.ENABLE_LOWERCASE, "false");
        runner.setProperty(NGSIToMySQL.BATCH_SIZE, "100");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "false");
        backend = new MySQLBackend();
    }


    /**
     * [NGSIToMySQL.configure] -------- enable_encoding can only be 'true' or 'false'.
     */
    @Test
    public void testBuildDBNameNoEncoding() throws Exception {

        System.out.println("[NGSIToMySQL.buildDBName]"
                + "-------- When no encoding, the DB name is equals to the encoding of the notified/defaulted service");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean(); // default
        String service = "someService";

        try {
            String builtSchemaName = backend.buildDbName(service,enableEncoding,enableLowercase);
            String expectedDBName = "someService";

            try {
                assertEquals(expectedDBName, builtSchemaName);
                System.out.println("[NGSIToMySQL.buildDBName]"
                        + "-  OK  - '" + expectedDBName + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMySQL.buildDBName]"
                        + "- FAIL - '" + expectedDBName + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildDBName]"
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildDBNameNoEncoding

    @Test
    public void testBuildDBNameEncoding() throws Exception {
        System.out.println("[NGSIToMySQL.buildDBName]"
                + "-------- When encoding, the DB name is equals to the encoding of the notified/defaulted service");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String service = "someService";

        try {
            String builtSchemaName = backend.buildDbName(service,enableEncoding,enableLowercase);
            String expectedDBName = "someService";

            try {
                assertEquals(expectedDBName, builtSchemaName);
                System.out.println("[NGSIToMySQL.buildDBName]"
                        + "-  OK  - '" + expectedDBName + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMySQL.buildDBName]"
                        + "- FAIL - '" + expectedDBName + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildDBName]"
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildDBNameEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByServicePathNoEncoding() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath,entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "somePath";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByServicePathNoEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByServicePathEncoding() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]\")\n" +
                "                + \"-------- When encoding and when a non root service-path is notified/defaulted and data_model is \"\n" +
                "+ \"'db-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath,entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "x002fsomePath";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByServicePathEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByEntityNoEncoding() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted and data_model is "
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
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "somePath_someId_someType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByEntityNoEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByEntityEncoding() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]"
                + "-------- When encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'db-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "x002fsomePathxffffsomeIdxffffsomeType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByEntityEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByServicePathNoEncoding() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]\")\n" +
                "                + \"-------- When no encoding and when a root service-path is notified/defaulted and data_model is \"\n" +
                "+ \"'db-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-service-path");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);
        String expecetedTableName = "";
        String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);

        try {
            assertEquals(expecetedTableName, builtTableName);
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - The table name was built when data_model='dm-by-service-path' and using the root "
                    + "service path");
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "-  OK  - The table name was not built when data_model='dm-by-service-path' and using the root "
                    + "service path");
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByServicePathNoEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByServicePathEncoding() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]\")\n" +
                "                + \"-------- When encoding and when a non root service-path is notified/defaulted and data_model is \"\n" +
                "+ \"'dm-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "x002f";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByServicePathEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByEntityNoEncoding() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted and data_model is "
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
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "someId_someType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>"
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        );
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByEntityNoEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByEntityEncoding() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]"
                + "-------- When encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'db-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "x002fxffffsomeIdxffffsomeType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMySQL.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByEntityEncoding

    @Test
    public void testBuildDbNameLength() throws Exception {
        System.out.println("[NGSIToMySQL.buildDbName]"
                + "-------- A database name length greater than 64 characters is detected");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String service = "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongService";

        try {
            backend.buildDbName(service,enableEncoding,enableLowercase);
            fail("[NGSIToMySQL.buildDbName]"
                    + "- FAIL - A database name length greater than 64 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildDbName]"
                    + "-  OK  - A database name length greater than 64 characters has been detected");
        } // try catch
    }
    @Test
    public void testBuildTableNameLengthDataModelByServicePath() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]"
                + "-------- When data model is by service path, a table name length greater than 64 characters is "
                + "detected");
        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-service-path");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooongServicePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            fail("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - A table name length greater than 64 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "-  OK  - A table name length greater than 64 characters has been detected");
        } // try catch
    } // testBuildTableNameLengthDataModelByServicePath

    @Test
    public void testBuildTableNameLengthDataModelByEntity() throws Exception {
        System.out.println("[NGSIToMySQL.buildTableName]"
                + "-------- When data model is by entity, a table name length greater than 64 characters is detected");
        runner.setProperty(NGSIToMySQL.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToMySQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToMySQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToMySQL.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooongServicePath";
        Entity entity = new Entity("tooLooooooooooooooooooooooooooongEntity", "someType",null);

        try {
            backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
            fail("[NGSIToMySQL.buildTableName]"
                    + "- FAIL - A table name length greater than 64 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToMySQL.buildTableName]"
                    + "-  OK  - A table name length greater than 64 characters has been detected");
        } // try catch
    } // testBuildTableNameLengthDataModelByEntity



}

