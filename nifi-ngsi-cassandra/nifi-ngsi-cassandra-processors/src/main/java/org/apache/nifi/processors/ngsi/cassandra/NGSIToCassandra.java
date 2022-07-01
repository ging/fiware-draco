package org.apache.nifi.processors.ngsi.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.cassandra.CassandraSessionProviderService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ngsi.cassandra.backends.CassandraBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIEvent;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIUtils;
import org.apache.nifi.util.StopWatch;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({"cassandra", "cql", "put", "insert", "update", "set", "record","NGSIv2", "NGSI","FIWARE"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This is a record aware processor that reads the content of the incoming FlowFile as individual records using the " +
        "configured 'Record Reader' and writes them to Apache Cassandra using native protocol version 3 or higher.")
public class NGSIToCassandra extends AbstractCassandraProcessor {
    private final CassandraBackend cassandra= new CassandraBackend();
    public static final PropertyDescriptor CQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("CQL select query")
            .description("CQL select query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-table")
            .displayName("Table name")
            .description("The name of the Cassandra table to which the records have to be written.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-batch-size")
            .displayName("Batch size")
            .description("Specifies the number of 'Insert statements' to be grouped together to execute as a batch (BatchStatement)")
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final PropertyDescriptor BATCH_STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-batch-statement-type")
            .displayName("Batch Statement Type")
            .description("Specifies the type of 'Batch Statement' to be used.")
            .allowableValues(BatchStatement.Type.values())
            .defaultValue(BatchStatement.Type.LOGGED.toString())
            .required(false)
            .build();

    static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractCassandraProcessor.CONSISTENCY_LEVEL)
            .allowableValues(ConsistencyLevel.SERIAL.name(), ConsistencyLevel.LOCAL_SERIAL.name())
            .defaultValue(ConsistencyLevel.SERIAL.name())
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors = Collections.unmodifiableList(Arrays.asList(
            CONNECTION_PROVIDER_SERVICE, NGSI_VERSION, DATA_MODEL , ATTR_PERSISTENCE,DEFAULT_SERVICE,DEFAULT_SERVICE_PATH,ENABLE_ENCODING, ENABLE_LOWERCASE,//CONTACT_POINTS, KEYSPACE, TABLE, USERNAME, PASSWORD,
            //RECORD_READER_FACTORY,
             BATCH_SIZE, CONSISTENCY_LEVEL, BATCH_STATEMENT_TYPE));

    private final static Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = session.get();

        if (inputFlowFile == null) {
            return;
        }
        String cassandraTable = "";
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final String batchStatementType = context.getProperty(BATCH_STATEMENT_TYPE).getValue();
        final String serialConsistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();

        final BatchStatement batchStatement;
        final Session connectionSession = cassandraSession.get();
        final AtomicInteger recordsAdded = new AtomicInteger(0);
        final StopWatch stopWatch = new StopWatch(true);
        final boolean connectionProviderIsSet = context.getProperty(CONNECTION_PROVIDER_SERVICE).isSet();
        if (connectionProviderIsSet) {
            CassandraSessionProviderService sessionProvider = context.getProperty(CONNECTION_PROVIDER_SERVICE).asControllerService(CassandraSessionProviderService.class);
            cluster.set(sessionProvider.getCluster());
            cassandraSession.set(sessionProvider.getCassandraSession());
            connectToCassandra(context);
        }

        boolean error = false;
        NGSIUtils n = new NGSIUtils();
        final NGSIEvent event=n.getEventFromFlowFile(inputFlowFile,session,context.getProperty(NGSI_VERSION).getValue(), false);
        final long creationTime = event.getCreationTime();
        final String fiwareService = (event.getFiwareService().compareToIgnoreCase("nd")==0)?context.getProperty(DEFAULT_SERVICE).getValue():event.getFiwareService();
        final String fiwareServicePath = (event.getFiwareServicePath().compareToIgnoreCase("/nd")==0)?context.getProperty(DEFAULT_SERVICE_PATH).getValue():event.getFiwareServicePath();
        try {
            final String dbName = cassandra.buildKeyspace(fiwareService, context.getProperty(ENABLE_ENCODING).asBoolean(), context.getProperty(ENABLE_LOWERCASE).asBoolean());
            for (Entity entity : event.getEntities()) {
                cassandraTable = cassandra.buildTableName(fiwareServicePath, entity, context.getProperty(DATA_MODEL).getValue(), context.getProperty(ENABLE_ENCODING).asBoolean(), context.getProperty(ENABLE_LOWERCASE).asBoolean());
                System.out.println(cassandraTable);
                String cql = cassandra.insertQuery(dbName,entity, creationTime, fiwareServicePath, cassandraTable, context.getProperty(ATTR_PERSISTENCE).getValue());
                System.out.println(cql);
                connectionSession.execute(cassandra.createKeyspace(dbName));
                connectionSession.execute(cassandra.createTable(dbName,cassandraTable,context.getProperty(ATTR_PERSISTENCE).getValue()));
                connectionSession.execute(cql);

            }   // Get or create the appropriate PreparedStatement to use.

        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }


        } catch (Exception e) {
            error = true;
            getLogger().error("Unable to write the records into Cassandra table due to {}", new Object[] {e});
            session.transfer(inputFlowFile, REL_FAILURE);
        } finally {
            if (!error) {
                stopWatch.stop();
                long duration = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                String transitUri = "cassandra://" + connectionSession.getCluster().getMetadata().getClusterName() + "." + cassandraTable;

                session.getProvenanceReporter().send(inputFlowFile, transitUri, "Inserted " + recordsAdded.get() + " records", duration);
                session.transfer(inputFlowFile, REL_SUCCESS);
            }
        }

    }

    @OnUnscheduled
    public void stop(ProcessContext context) {
        super.stop(context);
    }

    @OnShutdown
    public void shutdown(ProcessContext context) {
        super.stop(context);
    }

}