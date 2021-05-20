package org.apache.nifi.processors.ngsi;

import com.google.gson.JsonObject;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.ngsi.ngsi.aggregators.CKANAggregator;
import org.apache.nifi.processors.ngsi.ngsi.backends.ckan.CKANBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIEvent;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"CKAN","ckan","sql", "put", "rdbms", "database", "create", "insert", "relational","NGSIv2", "NGSI","FIWARE"})
@CapabilityDescription("Create a CKAN resource, package and dataset if not exits using the information coming from and NGSI event converted to flow file." +
        "After insert all of the vales of the flow file content extraction the entities and attributes")


public class NGSIToCKAN extends AbstractProcessor {
    protected static final PropertyDescriptor CKAN_HOST = new PropertyDescriptor.Builder()
            .name("CKAN Host")
            .displayName("CKAN Host")
            .description("FQDN/IP address where the CKAN server runs. Default value is localhost")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CKAN_PORT = new PropertyDescriptor.Builder()
            .name("CKAN Port")
            .displayName("CKAN Port")
            .description("Port where the CKAN server runs. Default value is 80")
            .required(true)
            .defaultValue("80")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CKAN_VIEWER = new PropertyDescriptor.Builder()
            .name("CKAN Viewer")
            .displayName("CKAN Viewer")
            .description("The CKAN resource page can contain one or more visualizations of the resource data or file contents (a table, a bar chart, a map, etc). These are commonly referred to as resource views.")
            .required(true)
            .defaultValue("recline_grid_view")
            .allowableValues("recline_view", "recline_grid_view","recline_graph_view","recline_map_view","text_view","image_view","video_view","audio_view","webpage_view")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CKAN_API_KEY = new PropertyDescriptor.Builder()
            .name("CKAN API Key")
            .displayName("CKAN API Key")
            .description("The APi Key you are going o use in CKAN")
            .required(true)
            .defaultValue("XXXXXX")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ORION_URL = new PropertyDescriptor.Builder()
            .name("ORION URL")
            .displayName("ORION URL")
            .description("To be put as the filestore URL.\n")
            .required(true)
            .defaultValue(" http://localhost:1026")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SSL = new PropertyDescriptor.Builder()
            .name("SSL")
            .displayName("SSL")
            .description("ssl for connection")
            .required(false)
            .defaultValue("false")
            .allowableValues("false", "true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DATA_MODEL = new PropertyDescriptor.Builder()
            .name("data-model")
            .displayName("Data Model")
            .description("The Data model for creating the tables when an event have been received you can choose between" +
                    ":db-by-service-path or db-by-entity for ngsiv2 and  db-by-entity or db-by-entity-type for ngsi-ld, default value is db-by-entity")
            .required(false)
            .allowableValues("db-by-entity-id", "db-by-entity")
            .defaultValue("db-by-entity")
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
            .description("The version of NGSI of your incomming events. You can choose Between v2 for NGSIv2 and ld for NGSI-LD ")
            .required(false)
            .allowableValues("v2","ld")
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

    protected static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max Connections")
            .description("Maximum number of connections allowed for a Http-based HDFS backend.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("500")
            .build();

    protected static final PropertyDescriptor MAX_CONNECTIONS_PER_ROUTE = new PropertyDescriptor.Builder()
            .name("Max Connections per Route")
            .description("Maximum number of connections per route allowed for a Http-based HDFS backend.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
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
        properties.add(CKAN_HOST);
        properties.add(CKAN_PORT);
        properties.add(CKAN_VIEWER);
        properties.add(CKAN_API_KEY);
        properties.add(ORION_URL);
        properties.add(SSL);
        properties.add(NGSI_VERSION);
        properties.add(DATA_MODEL);
        properties.add(ATTR_PERSISTENCE);
        properties.add(DEFAULT_SERVICE);
        properties.add(DEFAULT_SERVICE_PATH);
        properties.add(ENABLE_ENCODING);
        properties.add(ENABLE_LOWERCASE);
        properties.add(BATCH_SIZE);
        properties.add(MAX_CONNECTIONS);
        properties.add(MAX_CONNECTIONS_PER_ROUTE);
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

    protected void persistFlowFile(final ProcessContext context, final FlowFile flowFile, ProcessSession session) {

        final String[] host = {context.getProperty(CKAN_HOST).getValue()};
        final String port = context.getProperty(CKAN_PORT).getValue();
        final String apiKey = context.getProperty(CKAN_API_KEY).getValue();
        final String ckanViewer = context.getProperty(CKAN_VIEWER).getValue();
        final String orioUrl = context.getProperty(ORION_URL).getValue();
        final boolean ssl = context.getProperty(SSL).asBoolean();
        final int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
        final int maxConnectionsPerRoute = context.getProperty(MAX_CONNECTIONS_PER_ROUTE).asInteger();
        final boolean enableEncoding = context.getProperty(ENABLE_ENCODING).asBoolean();
        final boolean enableLowercase = context.getProperty(ENABLE_LOWERCASE).asBoolean();
        final String attrPersistence = context.getProperty(ATTR_PERSISTENCE).getValue();
        final CKANBackend ckanBackend = new CKANBackend(apiKey,host,port,orioUrl,ssl,maxConnections,maxConnectionsPerRoute,ckanViewer);
        final NGSIUtils n = new NGSIUtils();
        final String ngsiVersion=context.getProperty(NGSI_VERSION).getValue();
        final String dataModel=context.getProperty(DATA_MODEL).getValue();
        final NGSIEvent event=n.getEventFromFlowFile(flowFile,session,ngsiVersion);
        final long creationTime = event.getCreationTime();
        final String fiwareService = (event.getFiwareService().compareToIgnoreCase("nd")==0)?context.getProperty(DEFAULT_SERVICE).getValue():event.getFiwareService();
        final String fiwareServicePath = ("ld".equals(context.getProperty(NGSI_VERSION).getValue()))?"":(event.getFiwareServicePath().compareToIgnoreCase("/nd")==0)?context.getProperty(DEFAULT_SERVICE_PATH).getValue():event.getFiwareServicePath();
        CKANAggregator aggregator = new CKANAggregator() {
            @Override
            public void aggregate(Entity entity, long creationTime, String dataModel) {

            }
        };
        aggregator = aggregator.getAggregator(("row".equals(attrPersistence))?true:false);
        try {

            final String orgName = ckanBackend.buildOrgName(fiwareService,dataModel,enableEncoding,enableLowercase,ngsiVersion);
            ArrayList<Entity> entities= new ArrayList<>();
            entities = ("ld".equals(context.getProperty(NGSI_VERSION).getValue()))?event.getEntitiesLD():event.getEntities();
            getLogger().info("[] Persisting data at NGSICKANSink (orgName=" + orgName+ ", ");

            for (Entity entity : entities) {
                final String pkgName = ckanBackend.buildPkgName(fiwareService,entity,dataModel,enableEncoding,enableLowercase,ngsiVersion);
                final String resName = ckanBackend.buildResName(entity,dataModel,enableEncoding,enableLowercase,ngsiVersion);
                aggregator.initialize(entity,context.getProperty(NGSI_VERSION).getValue());
                aggregator.aggregate(entity, creationTime, context.getProperty(NGSI_VERSION).getValue());
                ArrayList<JsonObject> jsonObjects = CKANAggregator.linkedHashMapToJson(aggregator.getAggregationToPersist());
                String  aggregation= "";

                for (JsonObject jsonObject : jsonObjects) {
                    if (aggregation.isEmpty()) {
                        aggregation = jsonObject.toString();
                    } else {
                        aggregation += "," + jsonObject;
                    }
                }


                getLogger().info("[] Persisting data at NGSICKANSink (orgName=" + orgName
                                + ", pkgName=" + pkgName + ", resName=" + resName + ", data=(" + aggregation + ")");

                // Do try-catch only for metrics gathering purposes... after that, re-throw
                try {
                    if (aggregator instanceof CKANAggregator.RowAggregator) {
                        ckanBackend.persist(orgName, pkgName, resName, aggregation, true);
                    } else {
                        ckanBackend.persist(orgName, pkgName, resName, aggregation, false);
                    } // if else

                } catch (Exception e) {
                    throw e;
                } // catch
            } // for

        }catch (Exception e){
            getLogger().error(e.toString());
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        try {
            persistFlowFile(context, flowFile, session);
            logger.info("inserted {} into CKAN", new Object[]{flowFile});
            session.getProvenanceReporter().send(flowFile, "report");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("Failed to insert {} into CKAN due to {}", new Object[] {flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

}

