package org.apache.nifi.processors.ngsi;

import org.apache.nifi.processors.ngsi.ngsi.aggregators.HDFSAggregator;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestNGSIToHDFS {
    private TestRunner runner;
    private HDFSAggregator aggregator;


    @Before
    public void setUp() throws Exception {

        runner = TestRunners.newTestRunner(NGSIToHDFS.class);
        runner.setProperty(NGSIToHDFS.HDFS_HOST,"127.0.0.1");
        runner.setProperty(NGSIToHDFS.HDFS_PORT,"14000");
        runner.setProperty(NGSIToHDFS.HDFS_USERNAME,"hdfs");
        runner.setProperty(NGSIToHDFS.HDFS_PASSWORD,"hdfs");
        runner.setProperty(NGSIToHDFS.NGSI_VERSION,"v2");
        runner.setProperty(NGSIToHDFS.DATA_MODEL,"db-by-entity");
        runner.setProperty(NGSIToHDFS.ENABLE_ENCODING,"false");
        runner.setProperty(NGSIToHDFS.ENABLE_LOWERCASE,"false");
        runner.setProperty(NGSIToHDFS.OAUTH2_TOKEN,"xxxxxxxxxxxxx");
        runner.setProperty(NGSIToHDFS.SERVICE_AS_NAMESPACE,"false");
        runner.setProperty(NGSIToHDFS.FILE_FORMAT,"json-row");
        runner.setProperty(NGSIToHDFS.BACKEND_IMPL,"binary");
        runner.setProperty(NGSIToHDFS.BACKEND_MAX_CONNS,"500");
        runner.setProperty(NGSIToHDFS.BACKEND_MAX_CONNS_PER_ROUTE,"100");
        runner.setProperty(NGSIToHDFS.BATCH_SIZE,"1");
        runner.setProperty(NGSIToHDFS.BATCH_TIMEOUT,"30");
        runner.setProperty(NGSIToHDFS.BATCH_TTL,"10");
        runner.setProperty(NGSIToHDFS.BATCH_RETRY_INTERVAL,"5000");
        runner.setProperty(NGSIToHDFS.HIVE,"false");
        runner.setProperty(NGSIToHDFS.HIVE_SERVER_VERSION,"2");
        runner.setProperty(NGSIToHDFS.HIVE_HOST,"127.0.0.1");
        runner.setProperty(NGSIToHDFS.HIVE_PORT,"10000");
        runner.setProperty(NGSIToHDFS.HIVE_DB_TYPE,"default-db");
        runner.setProperty(NGSIToHDFS.KRB5_AUTH,"false");
        runner.setProperty(NGSIToHDFS.KRB5_USER,"krb");
        runner.setProperty(NGSIToHDFS.KRB5_PASSWORD,"krb");
        runner.setProperty(NGSIToHDFS.KRB5_LOGIN_CONF_FILE,"./");
        runner.setProperty(NGSIToHDFS.KRB5_CONF_FILE,"./");

    }

    @Test
    public void testBuildFolderPathNonRootServicePathNoEncoding() throws Exception {
        System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted the HDFS folder "
                + "path is the encoding of <service>/<service-path>/<entity>");

        boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToHDFS.ENABLE_ENCODING).asBoolean();
        String fileFormat = runner.getProcessContext().getProperty(NGSIToHDFS.FILE_FORMAT).getValue();
        String service = "someService";
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);
        String destination = entity.getEntityId()+"="+entity.getEntityType();
        aggregator = new HDFSAggregator() {
            @Override
            public void aggregate(long creationTime, Entity entity, String username) throws Exception {

            }
        }.getAggregator(fileFormat);

        try {
            String buildFolderPath = aggregator.buildFolderPath(service, servicePath, destination,enableEncoding);
            String expectedFolderPath = "someService/somePath/someId_someType";

            try {
                assertEquals(expectedFolderPath, buildFolderPath);
                System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                        + "-  OK  - '" + buildFolderPath + "' is equals to "
                        + "<service>/<service-path>/<entity>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                        + "- FAIL - '" + buildFolderPath + "' is not equals to "
                        + "<service>/<service-path>/<entity>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildFolderPathNonRootServicePathNoEncoding

    /**
     * [NGSIToHDFSTest.buildFolderPath] -------- When encoding and when a non root service-path is notified/defaulted
     * the HDFS folder path is the encoding of \<service\>/\<service-path\>/\<entity\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildFolderPathNonRootServicePathEncoding() throws Exception {
        System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                + "-------- When encoding and when a non root service-path is notified/defaulted the HDFS folder path "
                + "is the encoding of <service>/<service-path>/<entity>");

        runner.setProperty(NGSIToHDFS.ENABLE_ENCODING,"true");
        boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToHDFS.ENABLE_ENCODING).asBoolean();
        String fileFormat = runner.getProcessContext().getProperty(NGSIToHDFS.FILE_FORMAT).getValue();
        String service = "someService";
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);
        String destination = entity.getEntityId()+"="+entity.getEntityType();

        aggregator = new HDFSAggregator() {
            @Override
            public void aggregate(long creationTime, Entity entity, String username) throws Exception {

            }
        }.getAggregator(fileFormat);

        try {
            String buildFolderPath = aggregator.buildFolderPath(service, servicePath, destination,enableEncoding);
            String expectedFolderPath = "someService/somePath/someIdxffffsomeType";

            try {
                assertEquals(expectedFolderPath, buildFolderPath);
                System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                        + "-  OK  - '" + buildFolderPath + "' is equals to the encoding of "
                        + "<service>/<service-path>/<entity>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                        + "- FAIL - '" + buildFolderPath + "' is not equals to the encoding of "
                        + "<service>/<service-path>/<entity>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildFolderPathNonRootServicePathEncoding

    /**
     * [NGSIToHDFSTest.buildTableName] -------- When no encoding and when a root service-path is notified/defaulted
     * the HDFS folder path is the encoding of \<service\>/\<entity\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildFolderPathRootServicePathNoEncoding() throws Exception {
        System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                + "-------- When no encoding and when a root service-path is notified/defaulted the HDFS folder path "
                + "is the encoding of <service>/<entity>");
        runner.setProperty(NGSIToHDFS.ENABLE_ENCODING,"false");
        boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToHDFS.ENABLE_ENCODING).asBoolean();
        String fileFormat = runner.getProcessContext().getProperty(NGSIToHDFS.FILE_FORMAT).getValue();
        String service = "someService";
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);
        String destination = entity.getEntityId()+"="+entity.getEntityType();

        aggregator = new HDFSAggregator() {
            @Override
            public void aggregate(long creationTime, Entity entity, String username) throws Exception {

            }
        }.getAggregator(fileFormat);

        try {
            String builtTableName = aggregator.buildFolderPath(service, servicePath, destination, enableEncoding);
            String expecetedTableName = "someService/someId_someType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of "
                        + "<service>/<entity>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of "
                        + "<service>/<entity>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildFolderPathRootServicePathNoEncoding

    @Test
    public void testBuildFolderPathRootServicePathEncoding() throws Exception {
        System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                + "-------- When no encoding and when a root service-path is notified/defaulted the HDFS folder path "
                + "is the encoding of <service>/<entity>");
        runner.setProperty(NGSIToHDFS.ENABLE_ENCODING,"true");
        boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToHDFS.ENABLE_ENCODING).asBoolean();
        String fileFormat = runner.getProcessContext().getProperty(NGSIToHDFS.FILE_FORMAT).getValue();
        String service = "someService";
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);
        String destination = entity.getEntityId()+"="+entity.getEntityType();

        aggregator = new HDFSAggregator() {
            @Override
            public void aggregate(long creationTime, Entity entity, String username) throws Exception {

            }
        }.getAggregator(fileFormat);

        try {
            String builtTableName = aggregator.buildFolderPath(service, servicePath, destination,enableEncoding);
            String expecetedTableName = "someService/someIdxffffsomeType";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of "
                        + "<service>/<entity>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of "
                        + "<service>/<entity>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToHDFSTest.buildFolderPath]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildFolderPathRootServicePathEncoding

    @Test
    public void testBuildFilePathNonRootServicePathNoEncoding() throws Exception {
        System.out.println("[NGSIToHDFSTest.buildFilePath]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted the HDFS file path "
                + "is the encoding of <service>/<service-path>/<entity>/<entity>.txt");
        runner.setProperty(NGSIToHDFS.ENABLE_ENCODING,"false");
        boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToHDFS.ENABLE_ENCODING).asBoolean();
        String fileFormat = runner.getProcessContext().getProperty(NGSIToHDFS.FILE_FORMAT).getValue();
        String service = "someService";
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);
        String destination = entity.getEntityId()+"="+entity.getEntityType();

        aggregator = new HDFSAggregator() {
            @Override
            public void aggregate(long creationTime, Entity entity, String username) throws Exception {

            }
        }.getAggregator(fileFormat);

        try {
            String buildFolderPath = aggregator.buildFilePath(service, servicePath, destination,enableEncoding);
            String expectedFolderPath = "someService/somePath/someId_someType/someId_someType.txt";

            try {
                assertEquals(expectedFolderPath, buildFolderPath);
                System.out.println("[NGSIToHDFSTest.buildFilePath]"
                        + "-  OK  - '" + buildFolderPath + "' is equals to "
                        + "<service>/<service-path>/<entity>/<entity>.txt");
            } catch (AssertionError e) {
                System.out.println("[NGSIToHDFSTest.buildFilePath]"
                        + "- FAIL - '" + buildFolderPath + "' is not equals to "
                        + "<service>/<service-path>/<entity>/<entity>.txt");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToHDFSTest.buildFilePath]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildFilePathNonRootServicePathNoEncoding

    @Test
    public void testBuildFilePathNonRootServicePathEncoding() throws Exception {
        System.out.println("[NGSIToHDFSTest.buildFilePath]"
                + "-------- When no encoding and when a non root service-path is notified/defaulted the HDFS file path "
                + "is the encoding of <service>/<service-path>/<entity>/<entity>.txt");
        runner.setProperty(NGSIToHDFS.ENABLE_ENCODING,"true");
        boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToHDFS.ENABLE_ENCODING).asBoolean();
        String fileFormat = runner.getProcessContext().getProperty(NGSIToHDFS.FILE_FORMAT).getValue();
        String service = "someService";
        String servicePath = "/somePath";
        Entity entity = new Entity("someId","someType",null);
        String destination = entity.getEntityId()+"="+entity.getEntityType();

        aggregator = new HDFSAggregator() {
            @Override
            public void aggregate(long creationTime, Entity entity, String username) throws Exception {

            }
        }.getAggregator(fileFormat);

        try {
            String buildFolderPath = aggregator.buildFilePath(service, servicePath, destination,enableEncoding);
            String expectedFolderPath = "someService/somePath/someIdxffffsomeType/someIdxffffsomeType.txt";

            try {
                assertEquals(expectedFolderPath, buildFolderPath);
                System.out.println("[NGSIToHDFSTest.buildFilePath]"
                        + "-  OK  - '" + buildFolderPath + "' is equals to the encoding of "
                        + "<service>/<service-path>/<entity>/<entity>.txt");
            } catch (AssertionError e) {
                System.out.println("[NGSIToHDFSTest.buildFilePath]"
                        + "- FAIL - '" + buildFolderPath + "' is not equals to the encoding of "
                        + "<service>/<service-path>/<entity>/<entity>.txt");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToHDFSTest.buildFilePath]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildFilePathNonRootServicePathEncoding

    @Test
    public void testBuildFilePathRootServicePathNoEncoding() throws Exception {
        System.out.println("[NGSIToHDFSTest.buildFilePath]"
                + "-------- When no encoding and when a root service-path is notified/defaulted the HDFS file path is "
                + "the encoding of <service>/<entity>/<entity>.txt");
        runner.setProperty(NGSIToHDFS.ENABLE_ENCODING,"false");
        boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToHDFS.ENABLE_ENCODING).asBoolean();
        String fileFormat = runner.getProcessContext().getProperty(NGSIToHDFS.FILE_FORMAT).getValue();
        String service = "someService";
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);
        String destination = entity.getEntityId()+"="+entity.getEntityType();

        aggregator = new HDFSAggregator() {
            @Override
            public void aggregate(long creationTime, Entity entity, String username) throws Exception {

            }
        }.getAggregator(fileFormat);

        try {
            String builtTableName = aggregator.buildFilePath(service, servicePath, destination,enableEncoding);
            String expecetedTableName = "someService/someId_someType/someId_someType.txt";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToHDFSTest.buildFilePath]"
                        + "-  OK  - '" + builtTableName + "' is equals to "
                        + "<service>/<entity>/<entity>.txt");
            } catch (AssertionError e) {
                System.out.println("[NGSIToHDFSTest.buildFilePath]"
                        + "- FAIL - '" + builtTableName + "' is not equals to "
                        + "<service>/<entity>/<entity>.txt");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToHDFSTest.buildFilePath]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildFilePathRootServicePathNoEncoding

    @Test
    public void testBuildFilePathRootServicePathEncoding() throws Exception {
        System.out.println("[NGSIToHDFSTest.buildFilePath]"
                + "-------- When encoding and when a root service-path is notified/defaulted the HDFS file path is the "
                + "encoding of <service>/<entity>/<entity>.txt");
        runner.setProperty(NGSIToHDFS.ENABLE_ENCODING,"true");
        boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToHDFS.ENABLE_ENCODING).asBoolean();
        String fileFormat = runner.getProcessContext().getProperty(NGSIToHDFS.FILE_FORMAT).getValue();
        String service = "someService";
        String servicePath = "/";
        Entity entity = new Entity("someId","someType",null);
        String destination = entity.getEntityId()+"="+entity.getEntityType();

        aggregator = new HDFSAggregator() {
            @Override
            public void aggregate(long creationTime, Entity entity, String username) throws Exception {

            }
        }.getAggregator(fileFormat);

        try {
            String builtTableName = aggregator.buildFilePath(service, servicePath, destination,enableEncoding);
            String expecetedTableName = "someService/someIdxffffsomeType/someIdxffffsomeType.txt";

            try {
                assertEquals(expecetedTableName, builtTableName);
                System.out.println("[NGSIToHDFSTest.buildFilePath]"
                        + "-  OK  - '" + builtTableName + "' is equals to the encoding of "
                        + "<service>/<entity>/<entity>.txt");
            } catch (AssertionError e) {
                System.out.println("[NGSIToHDFSTest.buildFilePath]"
                        + "- FAIL - '" + builtTableName + "' is not equals to the encoding of "
                        + "<service>/<entity>/<entity>.txt");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToHDFSTest.buildFilePath]"
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildFilePathRootServicePathEncoding

    @Test
    public void testBuildFolderPathLength() throws Exception {
        System.out.println("[NGSIToHDFS.buildFolderPath]"
                + "-------- A folder path length greater than 255 characters is detected");
        runner.setProperty(NGSIToHDFS.ENABLE_ENCODING,"false");
        boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToHDFS.ENABLE_ENCODING).asBoolean();
        String fileFormat = runner.getProcessContext().getProperty(NGSIToHDFS.FILE_FORMAT).getValue();
        String service = "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooogService";
        String servicePath = "/tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongServicePath";
        String destination = "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooogDestination";

        aggregator = new HDFSAggregator() {
            @Override
            public void aggregate(long creationTime, Entity entity, String username) throws Exception {

            }
        }.getAggregator(fileFormat);

        try {
            aggregator.buildFolderPath(service, servicePath, destination,enableEncoding);
            fail("[NGSIToHDFS.buildFolderPath]"
                    + "- FAIL - A folder path length greater than 255 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToHDFS.buildFolderPath]"
                    + "-  OK  - A folder path length greater than 255 characters has been detected");
        } // try catch
    } // testBuildFolderPathLength

}
