package org.apache.nifi.processors.ngsi.dynamo;


import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.ngsi.dynamo.backends.DynamoBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assert.fail;


public class TestNGSIToDynamo {
    private TestRunner runner;
    private DynamoBackend backend;
    private ComponentLog logger;

    @Before
    public void setUp() throws Exception {

        runner = TestRunners.newTestRunner(NGSIToDynamo.class);
        runner.setProperty(NGSIToDynamo.NGSI_VERSION, "v2");
        runner.setProperty(NGSIToDynamo.DATA_MODEL, "db-by-service-path");
        runner.setProperty(NGSIToDynamo.ATTRIBUTE_PERSISTENCE, "row");
        runner.setProperty(NGSIToDynamo.DEFAULT_SERVICE_PATH, "/path");
        runner.setProperty(NGSIToDynamo.DEFAULT_SERVICE, "test");
        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToDynamo.ENABLE_LOWERCASE, "false");
        runner.setProperty(NGSIToDynamo.BATCH_SIZE, "100");
        logger =  runner.getLogger();
        backend = new DynamoBackend();
    }


    @Test
    public void testBuildTableNameNonRootServicePathDataModelByServicePathOldEncoding() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]"
                + "-------- When a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the Dynamo table name is the encoding of <service-path>");

        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();

        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);


        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase, logger);
            String expectedTableName = "somePath";

            try {
                assertEquals(expectedTableName, builtTableName);
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByServicePathOldEncoding


    @Test
    public void testBuildTableNameNonRootServicePathDataModelByServicePathNewEncoding() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]"
                + "-------- When a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the Dynamo table name is the encoding of <service-path>");

        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase, logger);
            String expectedTableName = "x002fsomePath";

            try {
                assertEquals(expectedTableName, builtTableName);
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByServicePathNewEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByEntityNoEncoding() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the Dynamo table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToDynamo.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase, logger);
            String expectedTableName = "somePath_someId_someType";

            try {
                assertEquals(expectedTableName, builtTableName);
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByEntityNoEncoding

    @Test
    public void testBuildTableNameNonRootServicePathDataModelByEntityEncoding() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]"
                + "-------- When encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'db-by-service-path' the Dynamo table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToDynamo.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase, logger);
            String expectedTableName = "x002fsomePathxffffsomeIdxffffsomeType";

            try {
                assertEquals(expectedTableName, builtTableName);
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameNonRootServicePathDataModelByEntityEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByServicePathNoEncoding() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]"
                + "-------- When a root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the Dynamo table name cannot be built");

        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToDynamo.DATA_MODEL, "db-by-service-path");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);
        String expectedTableName = "";

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase, logger);
            assertEquals(expectedTableName, builtTableName);
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - The table name was built when data_model='dm-by-service-path' and using the root "
                    + "service path");
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "-  OK  - The table name was not built when data_model='dm-by-service-path' and using the root "
                    + "service path");
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByServicePathNoEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByServicePathEncoding() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]\")\n" +
                "                + \"-------- When encoding and when a root service-path is notified/defaulted and data_model is \"\n" +
                "+ \"'dm-by-service-path' the Dynamo table name is the encoding of <service-path>");

        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId", "someType", null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase, logger);
            String expectedTableName = "x002f";

            try {
                assertEquals(expectedTableName, builtTableName);
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByServicePathEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByEntityNoEncoding() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the Dynamo table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToDynamo.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase, logger);
            String expectedTableName = "someId_someType";

            try {
                assertEquals(expectedTableName, builtTableName);
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>"
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                );
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByEntityNoEncoding

    @Test
    public void testBuildTableNameRootServicePathDataModelByEntityEncoding() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]"
                + "-------- When encoding and when a non root service-path is notified/defaulted and data_model is "
                + "'db-by-service-path' the Dynamo table name is the encoding of the concatenation of <service-path>, "
                + "<entityId> and <entityType>");

        runner.setProperty(NGSIToDynamo.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);

        try {
            String builtTableName = backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase, logger);
            String expectedTableName = "x002fxffffsomeIdxffffsomeType";

            try {
                assertEquals(expectedTableName, builtTableName);
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of <service-path>, <entityId> "
                        + "and <entityType>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToDynamo.buildTableName]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of <service-path>, "
                        + "<entityId> and <entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildTableNameRootServicePathDataModelByEntityEncoding

    @Test
    public void testBuildTableNameLengthDataModelByServicePath() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]"
                + "-------- When data model is by service path, a table name length greater than 255 characters is "
                + "detected");
        runner.setProperty(NGSIToDynamo.DATA_MODEL, "db-by-service-path");
        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongServicePath";
        Entity entity = new Entity("someId", "someType", null);

        try {
            backend.buildTableName(servicePath, entity,dataModel,enableEncoding,enableLowercase, logger);
            fail("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - A table name length greater than 255 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "-  OK  - A table name length greater than 255 characters has been detected");
        } // try catch
    } // testBuildTableNameLengthDataModelByServicePath

    @Test
    public void testBuildTableNameLengthDataModelByEntity() throws Exception {
        System.out.println("[NGSIToDynamo.buildTableName]"
                + "-------- When data model is by entity, a table name length greater than 255 characters is detected");
        runner.setProperty(NGSIToDynamo.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToDynamo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToDynamo.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToDynamo.DATA_MODEL).getValue();
        String servicePath = "/tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongServicePath";
        Entity entity = new Entity("/tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongEntity", "someType",null);

        try {
            backend.buildTableName(servicePath, entity, dataModel,enableEncoding,enableLowercase, logger);
            fail("[NGSIToDynamo.buildTableName]"
                    + "- FAIL - A table name length greater than 255 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToDynamo.buildTableName]"
                    + "-  OK  - A table name length greater than 255 characters has been detected");
        } // try catch
    } // testBuildTableNameLengthDataModelByEntity
}

