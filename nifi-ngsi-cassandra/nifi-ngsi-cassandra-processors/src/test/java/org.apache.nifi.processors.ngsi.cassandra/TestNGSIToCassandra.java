package org.apache.nifi.processors.ngsi.cassandra;

/**
 * @author anmunoz
 */

import org.apache.nifi.processors.ngsi.cassandra.backends.CassandraBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assert.fail;


public class TestNGSIToCassandra {
    private TestRunner runner;
    private CassandraBackend backend;

    @Before
    public void setUp() throws Exception {

        runner = TestRunners.newTestRunner(NGSIToCassandra.class);
        runner.setProperty(NGSIToCassandra.CONNECTION_PROVIDER_SERVICE, "cassandra-connection-provider");
        runner.setProperty(NGSIToCassandra.NGSI_VERSION, "v2");
        runner.setProperty(NGSIToCassandra.DATA_MODEL, "db-by-service-path");
        runner.setProperty(NGSIToCassandra.ATTR_PERSISTENCE, "row");
        runner.setProperty(NGSIToCassandra.DEFAULT_SERVICE_PATH, "/path");
        runner.setProperty(NGSIToCassandra.DEFAULT_SERVICE, "test");
        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToCassandra.ENABLE_LOWERCASE, "false");
        runner.setProperty(NGSIToCassandra.BATCH_SIZE, "100");
        backend = new CassandraBackend();
    }

    @Test
    public void testBuildKeyspaceNameNoEncoding() throws Exception {

        System.out.println("[NGSIToCassandra.buildKeyspaceName]"
                + "-------- When no encoding, the DB name is equals to the encoding of the notified/defaulted service");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean(); // default
        String service = "someService";

        try {
            String builtSchemaName = backend.buildKeyspace(service,enableEncoding,enableLowercase);
            String expectedKeyspaceName = "someService";

            try {
                assertEquals(expectedKeyspaceName, builtSchemaName);
                System.out.println("[NGSIToCassandra.buildKeyspaceName]"
                        + "-  OK  - '" + expectedKeyspaceName + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToCassandra.buildKeyspaceName]"
                        + "- FAIL - '" + expectedKeyspaceName + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildKeyspaceName]"
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testCreateKeySpace

    @Test
    public void testBuildKeyspaceEncoding() throws Exception {
        System.out.println("[NGSIToCassandra.buildKeyspace]"
                + "-------- When encoding, the DB name is equals to the encoding of the notified/defaulted service");
        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String service = "someService";

        try {
            String builtSchemaName = backend.buildKeyspace(service,enableEncoding,enableLowercase);
            String expectedKeyspace = "someService";

            try {
                assertEquals(expectedKeyspace, builtSchemaName);
                System.out.println("[NGSIToCassandra.buildKeyspace]"
                        + "-  OK  - '" + expectedKeyspace + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToCassandra.buildKeyspace]"
                        + "- FAIL - '" + expectedKeyspace + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildKeyspace]"
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildKeyspaceEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByServicePathNoEncoding() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath,entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "somePath";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByServicePathNoEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByServicePathEncoding() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]\")\n" +
                "                + \"-------- When encoding and when a non root service-path is notified/defaulted and data_model is \"\n" +
                "+ \"'db-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath,entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "x002fsomePath";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByServicePathEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByEntityNoEncoding() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToCassandra.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "somePath_someId_someType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByEntityNoEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByEntityEncoding() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]"
                + "-------- When encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'db-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToCassandra.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "x002fsomePathxffffsomeIdxffffsomeType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByEntityEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByServicePathNoEncoding() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]\")\n" +
                "                + \"-------- When no encoding and when a root service-path is notified/defaulted and data_model is \"\n" +
                "+ \"'db-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToCassandra.DATA_MODEL, "db-by-service-path");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);
        String expecetedTableName = "";
        String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);

        try {
            assertEquals(expecetedTableName, builtTableName);
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - The table name was built when data_model='dm-by-service-path' and using the root "
                    + "service path");
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "-  OK  - The table name was not built when data_model='dm-by-service-path' and using the root "
                    + "service path");
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByServicePathNoEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByServicePathEncoding() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]\")\n" +
                "                + \"-------- When encoding and when a non root service-path is notified/defaulted and data_model is \"\n" +
                "+ \"'dm-by-service-path' the MySQL table name is the encoding of <service-path>");

        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "x002f";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByServicePathEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByEntityNoEncoding() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToCassandra.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "someId_someType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>"
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                );
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByEntityNoEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByEntityEncoding() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]"
                + "-------- When encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'db-by-service-path' the MySQL table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToCassandra.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            String expecetedTableName = "x002fxffffsomeIdxffffsomeType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToCassandra.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByEntityEncoding

    @Test
    public void testBuildKeyspaceLength() throws Exception {
        System.out.println("[NGSIToCassandra.buildKeyspace]"
                + "-------- A database name length greater than 64 characters is detected");
        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String service = "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongService";

        try {
            backend.buildKeyspace(service,enableEncoding,enableLowercase);
            fail("[NGSIToCassandra.buildKeyspace]"
                    + "- FAIL - A database name length greater than 64 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildKeyspace]"
                    + "-  OK  - A database name length greater than 64 characters has been detected");
        } // try catch
    }
    @Test
    public void testBuildTableNameLengthDataModelByServicePath() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]"
                + "-------- When data model is by service path, a table name length greater than 64 characters is "
                + "detected");
        runner.setProperty(NGSIToCassandra.DATA_MODEL, "db-by-service-path");
        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooongServicePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase);
            fail("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - A table name length greater than 64 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "-  OK  - A table name length greater than 64 characters has been detected");
        } // try catch
    } // testBuildTableNameLengthDataModelByServicePath

    @Test
    public void testBuildTableNameLengthDataModelByEntity() throws Exception {
        System.out.println("[NGSIToCassandra.buildTableName]"
                + "-------- When data model is by entity, a table name length greater than 64 characters is detected");
        runner.setProperty(NGSIToCassandra.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToCassandra.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToCassandra.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToCassandra.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooongServicePath";
        Entity entity = new Entity("tooLooooooooooooooooooooooooooongEntity", "someType",null);

        try {
            backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase);
            fail("[NGSIToCassandra.buildTableName]"
                    + "- FAIL - A table name length greater than 64 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToCassandra.buildTableName]"
                    + "-  OK  - A table name length greater than 64 characters has been detected");
        } // try catch
    } // testBuildTableNameLengthDataModelByEntity

}