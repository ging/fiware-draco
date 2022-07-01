package org.apache.nifi.processors.ngsi;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ngsi.ngsi.aggregators.HDFSAggregator;
import org.apache.nifi.processors.ngsi.ngsi.backends.hdfs.HDFSBackend;
import org.apache.nifi.processors.ngsi.ngsi.backends.hdfs.HDFSBackendBinary;
import org.apache.nifi.processors.ngsi.ngsi.backends.hdfs.HDFSBackendREST;
import org.apache.nifi.processors.ngsi.ngsi.backends.HiveBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIEvent;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIUtils;

import java.util.*;


@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"HDFS", "put", "rdbms", "database", "hadoop", "insert", "relational","NGSIv2", "NGSI","FIWARE"})
@CapabilityDescription("Designed to persist NGSI-like context data events within a HDFS deployment. Usually, such a context data is notified by a Orion Context Broker instance, but could be any other system speaking the NGSI language.")

public class NGSIToHDFS extends AbstractProcessor {

     protected static final PropertyDescriptor HDFS_HOST = new PropertyDescriptor.Builder()
            .name("HDFS Host")
            .displayName("HDFS Host")
            .description("HDFS Host, without port")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor HDFS_PORT = new PropertyDescriptor.Builder()
            .name("HDFS Port")
            .displayName("HDFS Port")
            .description("HDFS Port, by desfaul is 14000")
            .required(true)
            .defaultValue("14000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor HDFS_USERNAME = new PropertyDescriptor.Builder()
            .name("HDFS User Name")
            .description("The HDFS username for authentication. If empty, no authentication is done.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DATA_MODEL = new PropertyDescriptor.Builder()
            .name("data-model")
            .displayName("Data Model")
            .description("The Data model for creating the tables when an event have been received you can choose between" +
                    ":db-by-service-path or db-by-entity, default value is db-by-service-path")
            .required(false)
            .allowableValues("db-by-entity")
            .defaultValue("db-by-entity")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor HDFS_PASSWORD = new PropertyDescriptor.Builder()
            .name("HDFS password")
            .description("The HDFS password for authentication. If empty, no authentication is done.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SERVICE_AS_NAMESPACE = new PropertyDescriptor.Builder()
            .name("Service as namespace")
            .displayName("Service as namespace")
            .description("If configured as true then the fiware-service (or the default one) is used as the HDFS namespace instead of hdfs_username, which in this case must be a HDFS superuser")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    protected static final PropertyDescriptor DEFAULT_SERVICE = new PropertyDescriptor.Builder()
            .name("default-service")
            .displayName("Default Service")
            .description("Default Fiware Service for building the database name")
            .required(false)
            .defaultValue("test")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DEFAULT_SERVICE_PATH = new PropertyDescriptor.Builder()
            .name("default-service-path")
            .displayName("Default Service path")
            .description("Default Fiware ServicePath for building the table name")
            .required(false)
            .defaultValue("/path")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ENABLE_ENCODING= new PropertyDescriptor.Builder()
            .name("enable-encoding")
            .displayName("Enable Encoding")
            .description("true or false, true applies the new encoding, false applies the old encoding.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    protected static final PropertyDescriptor ENABLE_LOWERCASE= new PropertyDescriptor.Builder()
            .name("enable-lowercase")
            .displayName("Enable Lowercase")
            .description("true or false, true for creating the Schema and Tables name with lowercase.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    protected static final PropertyDescriptor FILE_FORMAT = new PropertyDescriptor.Builder()
            .name("file-format")
            .displayName("File Format")
            .description("json-row, json-column, csv-row or json-column.")
            .required(false)
            .allowableValues("json-row", "json-column", "csv-row", "json-column")
            .defaultValue("json-row")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor BACKEND_IMPL = new PropertyDescriptor.Builder()
            .name("Backend Impl")
            .displayName("Backend Impl")
            .description("rest, if a WebHDFS/HttpFS-based implementation is used when interacting with HDFS; or binary, if a Hadoop API-based implementation is used when interacting with HDFS")
            .required(false)
            .allowableValues("rest", "binary")
            .defaultValue("rest")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor BACKEND_MAX_CONNS = new PropertyDescriptor.Builder()
            .name("Backend max connections")
            .displayName("Backend max connections")
            .description("Maximum number of connections allowed for a Http-based HDFS backend. Ignored if using a binary backend implementation.")
            .required(false)
            .defaultValue("500")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor BACKEND_MAX_CONNS_PER_ROUTE = new PropertyDescriptor.Builder()
            .name("Backend max connections per route")
            .displayName("Backend max connections per route")
            .description("Maximum number of connections per route allowed for a Http-based HDFS backend. Ignored if using a binary backend implementation.")
            .required(false)
            .defaultValue("100")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor BATCH_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Batch Timeout")
            .description("Number of seconds the batch will be building before it is persisted as it is.")
            .required(false)
            .defaultValue("30")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    protected static final PropertyDescriptor BATCH_TTL = new PropertyDescriptor.Builder()
            .name("Batch TTL")
            .description("Number of retries when a batch cannot be persisted. Use 0 for no retries, -1 for infinite retries. Please, consider an infinite TTL (even a very large one) may consume all the sink's channel capacity very quickly.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    protected static final PropertyDescriptor BATCH_RETRY_INTERVAL = new PropertyDescriptor.Builder()
            .name("Batch Retry Interval")
            .description("Comma-separated list of intervals (in miliseconds) at which the retries regarding not persisted batches will be done. First retry will be done as many miliseconds after as the first value, then the second retry will be done as many miliseconds after as second value, and so on. If the batch_ttl is greater than the number of intervals, the last interval is repeated.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    protected static final PropertyDescriptor OAUTH2_TOKEN = new PropertyDescriptor.Builder()
            .name("Oauth2 Token")
            .displayName("Oauth2 Token")
            .description("OAuth2 token required for the HDFS authentication.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor NGSI_VERSION = new PropertyDescriptor.Builder()
            .name("ngsi-version")
            .displayName("NGSI Version")
            .description("The version of NGSI of your incomming events. You can choose Between v2 for NGSIv2 and ld for NGSI-LD. NGSI-LD is not supported yet ")
            .required(false)
            .allowableValues("v2")
            .defaultValue("v2")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor HIVE = new PropertyDescriptor.Builder()
            .name("Hive")
            .displayName("Hive")
            .description("1 if the remote Hive server runs HiveServer1 or 2 if the remote Hive server runs HiveServer2.")
            .required(false)
            .allowableValues("true","false")
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor HIVE_SERVER_VERSION = new PropertyDescriptor.Builder()
            .name("hive.server_version")
            .displayName("hive.server_version")
            .description("1 if the remote Hive server runs HiveServer1 or 2 if the remote Hive server runs HiveServer2.")
            .required(false)
            .allowableValues("1","2")
            .defaultValue("2")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor HIVE_HOST = new PropertyDescriptor.Builder()
            .name("HIVE Host")
            .displayName("HIVE Host")
            .description("HIVE Host, without port")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor HIVE_PORT = new PropertyDescriptor.Builder()
            .name("HIVE Port")
            .displayName("HIVE Port")
            .description("HIVE Port, by desfaul is 10000")
            .required(false)
            .defaultValue("10000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor HIVE_DB_TYPE = new PropertyDescriptor.Builder()
            .name("hive.db_type")
            .displayName("hive.db_type")
            .description("default-db or namespace-db. If hive.db_type=default-db then the default Hive database is used. If hive.db_type=namespace-db and service_as_namespace=false then the hdfs_username is used as Hive database. If hive.db_type=namespace-db and service_as_namespace=true then the notified fiware-service is used as Hive database.")
            .required(false)
            .allowableValues("default-db","namespace-db")
            .defaultValue("default-db")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor KRB5_AUTH = new PropertyDescriptor.Builder()
            .name("krb5_auth")
            .displayName("krb5_auth")
            .description("true or false.")
            .required(false)
            .allowableValues("true","false")
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor KRB5_USER = new PropertyDescriptor.Builder()
            .name("krb5_user")
            .description("Ignored if krb5_auth=false, mandatory otherwise.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor KRB5_PASSWORD = new PropertyDescriptor.Builder()
            .name("krb5_password")
            .description("Ignored if krb5_auth=false, mandatory otherwise.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor KRB5_LOGIN_CONF_FILE = new PropertyDescriptor.Builder()
            .name("krb5_login_conf_file")
            .displayName("krb5_login_conf_file")
            .description("Ignored if krb5_auth=false.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor KRB5_CONF_FILE = new PropertyDescriptor.Builder()
            .name("krb5_conf_file")
            .displayName("krb5_conf_file")
            .description("Ignored if krb5_auth=false.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    protected static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HDFS_HOST);
        properties.add(HDFS_PORT);
        properties.add(HDFS_USERNAME);
        properties.add(HDFS_PASSWORD);
        properties.add(NGSI_VERSION);
        properties.add(DATA_MODEL);
        properties.add(DEFAULT_SERVICE);
        properties.add(DEFAULT_SERVICE_PATH);
        properties.add(ENABLE_ENCODING);
        properties.add(ENABLE_LOWERCASE);
        properties.add(OAUTH2_TOKEN);
        properties.add(SERVICE_AS_NAMESPACE);
        properties.add(FILE_FORMAT);
        properties.add(BACKEND_IMPL);
        properties.add(BACKEND_MAX_CONNS);
        properties.add(BACKEND_MAX_CONNS_PER_ROUTE);
        properties.add(BATCH_SIZE);
        properties.add(BATCH_TIMEOUT);
        properties.add(BATCH_TTL);
        properties.add(BATCH_RETRY_INTERVAL);
        properties.add(HIVE);
        properties.add(HIVE_SERVER_VERSION);
        properties.add(HIVE_HOST);
        properties.add(HIVE_PORT);
        properties.add(HIVE_DB_TYPE);
        properties.add(KRB5_AUTH);
        properties.add(KRB5_USER);
        properties.add(KRB5_PASSWORD);
        properties.add(KRB5_LOGIN_CONF_FILE);
        properties.add(KRB5_CONF_FILE);

        return properties;
    }

    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_RETRY);
        rels.add(REL_FAILURE);
        return rels;
    }

    protected void persistFlowFile(final ProcessContext context, final FlowFile flowFile,ProcessSession session) {

        final String[] host = {context.getProperty(HDFS_HOST).getValue()};
        final String port = context.getProperty(HDFS_PORT).getValue();
        final String username = context.getProperty(HDFS_USERNAME).getValue();
        final String password = context.getProperty(HDFS_PASSWORD).getValue();
        final String fileFormat = context.getProperty(FILE_FORMAT).getValue();
        final String oauth2Token = context.getProperty(OAUTH2_TOKEN).getValue();
        final boolean enableHive = context.getProperty(HIVE).asBoolean();
        final String hiveServerVersion = context.getProperty(HIVE_SERVER_VERSION).getValue();
        final String hiveHost = context.getProperty(HIVE_HOST).getValue();
        final String hivePort = context.getProperty(HIVE_PORT).getValue();
        final boolean enableKrb5 = context.getProperty(KRB5_AUTH).asBoolean();
        final String krb5User = context.getProperty(KRB5_USER).getValue();
        final String krb5Password = context.getProperty(KRB5_PASSWORD).getValue();
        final String krb5LoginConfFile = context.getProperty(KRB5_LOGIN_CONF_FILE).getValue();
        final String krb5ConfFile = context.getProperty(KRB5_CONF_FILE).getValue();
        final boolean serviceAsNamespace = context.getProperty(SERVICE_AS_NAMESPACE).asBoolean();
        final String backendImpl = context.getProperty(BACKEND_IMPL).getValue();
        final boolean enableEncoding = context.getProperty(ENABLE_ENCODING).asBoolean();
        final boolean enableLowercase = context.getProperty(ENABLE_LOWERCASE).asBoolean();
        final String hiveDBType = context.getProperty(HIVE_DB_TYPE).getValue();
        HDFSBackend persistenceBackend = null;
        HiveBackend hiveBackend = null;
        NGSIUtils n = new NGSIUtils();
        persistenceBackend = (backendImpl.compareToIgnoreCase("rest")==1)
                ?new HDFSBackendREST(host,port,username,oauth2Token,enableKrb5,krb5User,krb5Password,krb5LoginConfFile,krb5ConfFile,serviceAsNamespace)
                :new HDFSBackendBinary(host,port,username,password,oauth2Token,hiveServerVersion,hiveHost,
                hivePort,enableKrb5,krb5User,krb5Password,krb5LoginConfFile,krb5ConfFile,serviceAsNamespace);

        final NGSIEvent event=n.getEventFromFlowFile(flowFile,session,context.getProperty(NGSI_VERSION).getValue(), false);
        final String fiwareService = (event.getFiwareService().compareToIgnoreCase("nd")==0)?context.getProperty(DEFAULT_SERVICE).getValue():event.getFiwareService();
        final String fiwareServicePath = (event.getFiwareServicePath().compareToIgnoreCase("/nd")==0)?context.getProperty(DEFAULT_SERVICE_PATH).getValue():event.getFiwareServicePath();

        hiveBackend = new HiveBackend(hiveServerVersion, hiveHost, hivePort, username, password);

        try {

            for (Entity entity : event.getEntities()) {
                HDFSAggregator aggregator = new HDFSAggregator() {
                    @Override
                    public void aggregate(long creationTime, Entity entity, String username) throws Exception {

                    }

                    @Override
                    public String buildFolderPath(String service, String servicePath, String destination, boolean enableEncoding) throws Exception {
                        return super.buildFolderPath(service, servicePath, destination, enableEncoding);
                    }
                };

                aggregator = aggregator.getAggregator(fileFormat);
                aggregator.initialize(fiwareService,fiwareServicePath , entity, enableEncoding);
                aggregator.persistAggregation(aggregator, enableLowercase,persistenceBackend );

                if (fileFormat.equalsIgnoreCase("CSVROW")  || fileFormat.equalsIgnoreCase("CSVCOLUMN")) {
                    aggregator.persistMDAggregations(aggregator,persistenceBackend);
                }

                if (enableHive) {
                    if (hiveDBType.equalsIgnoreCase("namespace-db")) {
                        if (serviceAsNamespace) {
                            hiveBackend.doCreateDatabase(aggregator.getService());
                            aggregator.provisionHiveTable(aggregator, aggregator.getService(), enableLowercase,
                                    fileFormat,serviceAsNamespace,hiveHost,hivePort,hiveServerVersion,username,password);
                        } else {
                            hiveBackend.doCreateDatabase(username);
                            aggregator.provisionHiveTable(aggregator, aggregator.getService(), enableLowercase,
                                    fileFormat,serviceAsNamespace,hiveHost,hivePort,hiveServerVersion,username,password);
                        } // if else
                    } else {
                        aggregator.provisionHiveTable(aggregator, "default", enableLowercase,
                                fileFormat,serviceAsNamespace,hiveHost,hivePort,hiveServerVersion,username,password);
                    } // if else
                } // if
            } // for

        }catch (Exception e){
            getLogger().error(e.toString());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        try {
            persistFlowFile(context, flowFile, session);
            logger.info("inserted {} into HDFS", new Object[]{flowFile});
            session.getProvenanceReporter().send(flowFile, "report");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("Failed to insert {} into HDFS due to {}", new Object[] {flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }


}
