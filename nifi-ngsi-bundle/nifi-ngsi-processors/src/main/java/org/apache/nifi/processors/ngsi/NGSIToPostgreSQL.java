package org.apache.nifi.processors.ngsi;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ngsi.ngsi.aggregators.PostgreSQLAggregator;
import org.apache.nifi.processors.ngsi.ngsi.backends.postgresql.PostgreSQLBackendImpl;
import org.apache.nifi.processors.ngsi.errors.DracoBadConfiguration;
import org.apache.nifi.processor.util.pattern.*;
import org.apache.nifi.processor.util.pattern.PartialFunctions.FlowFileGroup;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIEvent;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIUtils;
import org.apache.nifi.processors.standard.util.JdbcCommon;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.apache.nifi.processor.util.pattern.ExceptionHandler.createOnError;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Postgresql","sql", "put", "rdbms", "database", "create", "insert", "relational","NGSIv2", "NGSI","FIWARE"})
@CapabilityDescription("Create a Data Base if not exits using the information coming from and NGSI event converted to flow file." +
        "After insert all of the vales of the flow file content extraction the entities and attributes")

@WritesAttributes({
        @WritesAttribute(attribute = "sql.generated.key", description = "If the database generated a key for an INSERT statement and the Obtain Generated Keys property is set to true, "
                + "this attribute will be added to indicate the generated key, if possible. This feature is not supported by all database vendors.")
})



public class NGSIToPostgreSQL extends AbstractProcessor {
    static final PropertyDescriptor PostgreSQL_HOST = new PropertyDescriptor.Builder()
        .name("PostgreSQL Host")
        .displayName("PostgreSQL Host")
        .description("PostgreSQL Host, without port")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
        
     static final PropertyDescriptor PostgreSQL_PORT = new PropertyDescriptor.Builder()
        .name("PostgreSQL Port")
        .displayName("PostgreSQL Port")
        .description("PostgreSQL Port, by desfaul is 5432")
        .required(true)
        .defaultValue("5432")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
        
     static final PropertyDescriptor PostgreSQL_USERNAME = new PropertyDescriptor.Builder()
        .name("PostgreSQL User Name")
        .description("The PostgreSQL username for authentication. If empty, no authentication is done.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
            
    static final PropertyDescriptor PostgreSQL_PASSWORD = new PropertyDescriptor.Builder()
        .name("PostgreSQL password")
        .description("The PostgreSQL password for authentication. If empty, no authentication is done.")
        .required(false)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor DATA_MODEL = new PropertyDescriptor.Builder()
            .name("data-model")
            .displayName("Data Model")
            .description("The Data model for creating the tables when an event have been received you can choose between" +
                    ":db-by-service-path or db-by-entity, default value is db-by-service-path")
            .required(false)
            .allowableValues("db-by-service-path", "db-by-entity")
            .defaultValue("db-by-service-path")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ATTR_PERSISTENCE = new PropertyDescriptor.Builder()
            .name("attr-persistence")
            .displayName("Attribute Persistence")
            .description("The mode of storing the data inside of the table")
            .required(false)
            .allowableValues("row", "column")
            .defaultValue("row")
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

    protected static final PropertyDescriptor TRANSACTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Transaction Timeout")
            .description("If the <Support Fragmented Transactions> property is set to true, specifies how long to wait for all FlowFiles for a particular fragment.identifier attribute "
                    + "to arrive before just transferring all of the FlowFiles with that identifier to the 'failure' relationship")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
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

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PostgreSQL_HOST);
        properties.add(PostgreSQL_PORT);
        properties.add(PostgreSQL_USERNAME);
        properties.add(PostgreSQL_PASSWORD);
        properties.add(NGSI_VERSION);
        properties.add(DATA_MODEL);
        properties.add(ATTR_PERSISTENCE);
        properties.add(DEFAULT_SERVICE);
        properties.add(DEFAULT_SERVICE_PATH);
        properties.add(ENABLE_ENCODING);
        properties.add(ENABLE_LOWERCASE);
        properties.add(BATCH_SIZE);
        properties.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_RETRY);
        rels.add(REL_FAILURE);
        return rels;
    }

    private static class FunctionContext extends RollbackOnFailure {
        private boolean obtainKeys = false;
        private boolean fragmentedTransaction = false;
        private boolean originalAutoCommit = false;
        private final long startNanos = System.nanoTime();

        private FunctionContext(boolean rollbackOnFailure) {
            super(rollbackOnFailure, true);
        }

        private boolean isSupportBatching() {
            return !obtainKeys && !fragmentedTransaction;
        }
    }

    protected void persistFlowFile(final ProcessContext context, final FlowFile flowFile,ProcessSession session) {

        final String host = context.getProperty(PostgreSQL_HOST).getValue();
        final String port = context.getProperty(PostgreSQL_PORT).getValue();
        final String username = context.getProperty(PostgreSQL_USERNAME).getValue();
        final String password = context.getProperty(PostgreSQL_PASSWORD).getValue();
        final String attrPersistence = context.getProperty(ATTR_PERSISTENCE).getValue();
        final String dataModel = context.getProperty(DATA_MODEL).getValue();
        final boolean enableEncoding = context.getProperty(ENABLE_ENCODING).asBoolean();
        final boolean enableLowercase = context.getProperty(ENABLE_LOWERCASE).asBoolean();

        NGSIUtils n = new NGSIUtils();
        final NGSIEvent event=n.getEventFromFlowFile(flowFile,session,context.getProperty(NGSI_VERSION).getValue());
        final String fiwareService = (event.getFiwareService().compareToIgnoreCase("nd")==0)?context.getProperty(DEFAULT_SERVICE).getValue():event.getFiwareService();
        final String fiwareServicePath = (event.getFiwareServicePath().compareToIgnoreCase("/nd")==0)?context.getProperty(DEFAULT_SERVICE_PATH).getValue():event.getFiwareServicePath();

        final long creationTime = event.getCreationTime();
            
        PostgreSQLBackendImpl persistenceBackend = new PostgreSQLBackendImpl(host, port, username, password);
            
        try {

            for (Entity entity : event.getEntities()) {
                String destination = entity.getEntityId();
                    
                PostgreSQLAggregator aggregator = new PostgreSQLAggregator() {
                    @Override
                    public void aggregate(long creationTime, Entity entity, String fiwareServicePath) {
                    }
                };
                aggregator = aggregator.getAggregator(attrPersistence);
                aggregator.initialize(fiwareService, fiwareServicePath , entity, dataModel, enableEncoding);
                aggregator.aggregate(creationTime, entity , fiwareServicePath);
                aggregator.persistAggregation(aggregator, enableLowercase, persistenceBackend );
                }
            }catch (Exception e){
                getLogger().error(e.toString());
            }
        }
    };
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        try {
            persistFlowFile(context, flowFile, session);
            logger.info("inserted {} into PostgreSQL", new Object[]{flowFile});
            session.getProvenanceReporter().send(flowFile, "report");
            session.transfer(flowFile, REL_SUCCESS);
            final Boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        } catch (Exception e) {
            logger.error("Failed to insert {} into PostgreSQL due to {}", new Object[] {flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        } //try-catch
            
    }
}
