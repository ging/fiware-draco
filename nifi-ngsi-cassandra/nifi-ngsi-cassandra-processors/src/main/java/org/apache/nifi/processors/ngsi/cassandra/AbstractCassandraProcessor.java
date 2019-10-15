package org.apache.nifi.processors.ngsi.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.cassandra.CassandraSessionProviderService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.SslContextFactory;


import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCassandraProcessor is a base class for Cassandra processors and contains logic and variables common to most
 * processors integrating with Apache Cassandra.
 */
public abstract class AbstractCassandraProcessor extends AbstractProcessor {

    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    // Common descriptors
    static final PropertyDescriptor CONNECTION_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("cassandra-connection-provider")
            .displayName("Cassandra Connection Provider")
            .description("Specifies the Cassandra connection providing controller service to be used to connect to Cassandra cluster.")
            .required(false)
            .identifiesControllerService(CassandraSessionProviderService.class)
            .build();

    static final PropertyDescriptor NGSI_VERSION = new PropertyDescriptor.Builder()
            .name("ngsi-version")
            .displayName("NGSI Version")
            .description("The version of NGSI of your incomming events. You can choose Between v2 for NGSIv2 and ld for NGSI-LD. NGSI-LD is not supported yet ")
            .required(false)
            .allowableValues("v2")
            .defaultValue("v2")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DATA_MODEL = new PropertyDescriptor.Builder()
            .name("data-model")
            .displayName("Data Model")
            .description("The Data model for creating the tables when an event have been received you can choose between" +
                    ":db-by-service-path or db-by-entity, default value is db-by-service-path")
            .required(false)
            .allowableValues("db-by-service-path", "db-by-entity")
            .defaultValue("db-by-service-path")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ATTR_PERSISTENCE = new PropertyDescriptor.Builder()
            .name("attr-persistence")
            .displayName("Attribute Persistence")
            .description("The mode of storing the data inside of the table")
            .required(false)
            .allowableValues("row")
            .defaultValue("row")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DEFAULT_SERVICE = new PropertyDescriptor.Builder()
            .name("default-service")
            .displayName("Default Service")
            .description("Default Fiware Service for building the database name")
            .required(false)
            .defaultValue("test")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ENABLE_ENCODING= new PropertyDescriptor.Builder()
            .name("enable-encoding")
            .displayName("Enable Encoding")
            .description("true or false, true applies the new encoding, false applies the old encoding.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor ENABLE_LOWERCASE= new PropertyDescriptor.Builder()
            .name("enable-lowercase")
            .displayName("Enable Lowercase")
            .description("true or false, true for creating the Schema and Tables name with lowercase.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor DEFAULT_SERVICE_PATH = new PropertyDescriptor.Builder()
            .name("default-service-path")
            .displayName("Default Service path")
            .description("Default Fiware ServicePath for building the table name")
            .required(false)
            .defaultValue("/path")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("Cassandra Contact Points")
            .description("Contact points are addresses of Cassandra nodes. The list of contact points should be "
                    + "comma-separated and in hostname:port format. Example node1:port,node2:port,...."
                    + " The default client port for Cassandra is 9042, but the port(s) must be explicitly specified.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .build();

    static final PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("Keyspace")
            .description("The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to " +
                    "include the keyspace name before any table reference, in case of 'query' native processors or " +
                    "if the processor exposes the 'Table' property, the keyspace name has to be provided with the " +
                    "table name in the form of <KEYSPACE>.<TABLE>")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Cassandra cluster")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Cassandra cluster")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consistency Level")
            .description("The strategy for how many replicas must respond before results are returned.")
            .required(false)
            .allowableValues(ConsistencyLevel.values())
            .defaultValue("ONE")
            .build();

    static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("Enable compression at transport-level requests and responses")
            .required(false)
            .allowableValues(ProtocolOptions.Compression.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("NONE")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred to this relationship if the operation failed.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is transferred to this relationship if the operation cannot be completed but attempting "
                    + "it again may succeed.")
            .build();

    protected static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(CONNECTION_PROVIDER_SERVICE);
        descriptors.add(NGSI_VERSION);
        descriptors.add(DATA_MODEL);
        descriptors.add(ATTR_PERSISTENCE);
        descriptors.add(DEFAULT_SERVICE);
        descriptors.add(DEFAULT_SERVICE_PATH);
        descriptors.add(ENABLE_ENCODING);
        descriptors.add(ENABLE_LOWERCASE);

    }

    protected final AtomicReference<Cluster> cluster = new AtomicReference<>(null);
    protected final AtomicReference<Session> cassandraSession = new AtomicReference<>(null);

    protected static final CodecRegistry codecRegistry = new CodecRegistry();

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Ensure that if username or password is set, then the other is too
        String userName = validationContext.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        String password = validationContext.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        if (StringUtils.isEmpty(userName) != StringUtils.isEmpty(password)) {
            results.add(new ValidationResult.Builder().subject("Username / Password configuration").valid(false).explanation(
                    "If username or password is specified, then the other must be specified as well").build());
        }

        // Ensure that both Connection provider service and the processor specific configurations are not provided
        boolean connectionProviderIsSet = validationContext.getProperty(CONNECTION_PROVIDER_SERVICE).isSet();
        boolean contactPointsIsSet = validationContext.getProperty(CONTACT_POINTS).isSet();

        if (connectionProviderIsSet && contactPointsIsSet) {
            results.add(new ValidationResult.Builder().subject("Cassandra configuration").valid(false).explanation("both " + CONNECTION_PROVIDER_SERVICE.getDisplayName() +
                    " and processor level Cassandra configuration cannot be provided at the same time.").build());
        }

        if (!connectionProviderIsSet && !contactPointsIsSet) {
            results.add(new ValidationResult.Builder().subject("Cassandra configuration").valid(false).explanation("either " + CONNECTION_PROVIDER_SERVICE.getDisplayName() +
                    " or processor level Cassandra configuration has to be provided.").build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        final boolean connectionProviderIsSet = context.getProperty(CONNECTION_PROVIDER_SERVICE).isSet();

        if (connectionProviderIsSet) {
            CassandraSessionProviderService sessionProvider = context.getProperty(CONNECTION_PROVIDER_SERVICE).asControllerService(CassandraSessionProviderService.class);
            cluster.set(sessionProvider.getCluster());
            cassandraSession.set(sessionProvider.getCassandraSession());
            return;
        }

        try {
            connectToCassandra(context);
        } catch (NoHostAvailableException nhae) {
            getLogger().error("No host in the Cassandra cluster can be contacted successfully to execute this statement", nhae);
            getLogger().error(nhae.getCustomMessage(10, true, false));
            throw new ProcessException(nhae);
        } catch (AuthenticationException ae) {
            getLogger().error("Invalid username/password combination", ae);
            throw new ProcessException(ae);
        }
    }

    public void connectToCassandra(ProcessContext context) {
        if (cluster.get() == null) {
            ComponentLog log = getLogger();
            final String contactPointList = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();
            final String consistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();
            final String compressionType = context.getProperty(COMPRESSION_TYPE).getValue();
            List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

            // Set up the client for secure (SSL/TLS communications) if configured to do so
            final SSLContext sslContext= null;;


            final String username, password;
            PropertyValue usernameProperty = context.getProperty(USERNAME).evaluateAttributeExpressions();
            PropertyValue passwordProperty = context.getProperty(PASSWORD).evaluateAttributeExpressions();

            if (usernameProperty != null && passwordProperty != null) {
                username = usernameProperty.getValue();
                password = passwordProperty.getValue();
            } else {
                username = null;
                password = null;
            }

            // Create the cluster and connect to it
            Cluster newCluster = createCluster(contactPoints, sslContext, username, password, compressionType);
            PropertyValue keyspaceProperty = context.getProperty(KEYSPACE).evaluateAttributeExpressions();

            final Session newSession;
            if (keyspaceProperty != null) {
                newSession = newCluster.connect(keyspaceProperty.getValue());
            } else {
                newSession = newCluster.connect();
            }

            newCluster.getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel));
            Metadata metadata = newCluster.getMetadata();

            log.info("Connected to Cassandra cluster: {}", new Object[]{metadata.getClusterName()});

            cluster.set(newCluster);
            cassandraSession.set(newSession);
        }
    }

    /**
     * Uses a Cluster.Builder to create a Cassandra cluster reference using the given parameters
     *
     * @param contactPoints The contact points (hostname:port list of Cassandra nodes)
     * @param sslContext    The SSL context (used for secure connections)
     * @param username      The username for connection authentication
     * @param password      The password for connection authentication
     * @param compressionType Enable compression at transport-level requests and responses.
     * @return A reference to the Cluster object associated with the given Cassandra configuration
     */
    protected Cluster createCluster(List<InetSocketAddress> contactPoints, SSLContext sslContext,
                                    String username, String password, String compressionType) {
        Cluster.Builder builder = Cluster.builder().addContactPointsWithPorts(contactPoints);
        if (sslContext != null) {
            JdkSSLOptions sslOptions = JdkSSLOptions.builder()
                    .withSSLContext(sslContext)
                    .build();
            builder = builder.withSSL(sslOptions);
            if(ProtocolOptions.Compression.SNAPPY.equals(compressionType)) {
                builder = builder.withCompression(ProtocolOptions.Compression.SNAPPY);
            } else if(ProtocolOptions.Compression.LZ4.equals(compressionType)) {
                builder = builder.withCompression(ProtocolOptions.Compression.LZ4);
            }
        }
        if (username != null && password != null) {
            builder = builder.withCredentials(username, password);
        }
        return builder.build();
    }

    public void stop(ProcessContext context) {
        // We don't want to close the connection when using 'Cassandra Connection Provider'
        // because each time @OnUnscheduled/@OnShutdown annotated method is triggered on a
        // processor, the connection would be closed which is not ideal for a centralized
        // connection provider controller service
        if (!context.getProperty(CONNECTION_PROVIDER_SERVICE).isSet()) {
            if (cassandraSession.get() != null) {
                cassandraSession.get().close();
                cassandraSession.set(null);
            }
            if (cluster.get() != null) {
                cluster.get().close();
                cluster.set(null);
            }
        }
    }


    protected static Object getCassandraObject(Row row, int i, DataType dataType) {
        if (dataType.equals(DataType.blob())) {
            return row.getBytes(i);

        } else if (dataType.equals(DataType.varint()) || dataType.equals(DataType.decimal())) {
            // Avro can't handle BigDecimal and BigInteger as numbers - it will throw an
            // AvroRuntimeException such as: "Unknown datum type: java.math.BigDecimal: 38"
            return row.getObject(i).toString();

        } else if (dataType.equals(DataType.cboolean())) {
            return row.getBool(i);

        } else if (dataType.equals(DataType.cint())) {
            return row.getInt(i);

        } else if (dataType.equals(DataType.bigint())
                || dataType.equals(DataType.counter())) {
            return row.getLong(i);

        } else if (dataType.equals(DataType.ascii())
                || dataType.equals(DataType.text())
                || dataType.equals(DataType.varchar())) {
            return row.getString(i);

        } else if (dataType.equals(DataType.cfloat())) {
            return row.getFloat(i);

        } else if (dataType.equals(DataType.cdouble())) {
            return row.getDouble(i);

        } else if (dataType.equals(DataType.timestamp())) {
            return row.getTimestamp(i);

        } else if (dataType.equals(DataType.date())) {
            return row.getDate(i);

        } else if (dataType.equals(DataType.time())) {
            return row.getTime(i);

        } else if (dataType.isCollection()) {

            List<DataType> typeArguments = dataType.getTypeArguments();
            if (typeArguments == null || typeArguments.size() == 0) {
                throw new IllegalArgumentException("Column[" + i + "] " + dataType.getName()
                        + " is a collection but no type arguments were specified!");
            }
            // Get the first type argument, to be used for lists and sets (and the first in a map)
            DataType firstArg = typeArguments.get(0);
            TypeCodec firstCodec = codecRegistry.codecFor(firstArg);
            if (dataType.equals(DataType.set(firstArg))) {
                return row.getSet(i, firstCodec.getJavaType());
            } else if (dataType.equals(DataType.list(firstArg))) {
                return row.getList(i, firstCodec.getJavaType());
            } else {
                // Must be an n-arg collection like map
                DataType secondArg = typeArguments.get(1);
                TypeCodec secondCodec = codecRegistry.codecFor(secondArg);
                if (dataType.equals(DataType.map(firstArg, secondArg))) {
                    return row.getMap(i, firstCodec.getJavaType(), secondCodec.getJavaType());
                }
            }

        } else {
            // The different types that we support are numbers (int, long, double, float),
            // as well as boolean values and Strings. Since Avro doesn't provide
            // timestamp types, we want to convert those to Strings. So we will cast anything other
            // than numbers or booleans to strings by using the toString() method.
            return row.getObject(i).toString();
        }
        return null;
    }

    /**
     * This method will create a schema a union field consisting of null and the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getUnionFieldType(String dataType) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(getSchemaForType(dataType)).endUnion();
    }

    /**
     * This method will create an Avro schema for the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getSchemaForType(String dataType) {
        SchemaBuilder.TypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();
        Schema returnSchema;
        switch (dataType) {
            case "string":
                returnSchema = typeBuilder.stringType();
                break;
            case "boolean":
                returnSchema = typeBuilder.booleanType();
                break;
            case "int":
                returnSchema = typeBuilder.intType();
                break;
            case "long":
                returnSchema = typeBuilder.longType();
                break;
            case "float":
                returnSchema = typeBuilder.floatType();
                break;
            case "double":
                returnSchema = typeBuilder.doubleType();
                break;
            case "bytes":
                returnSchema = typeBuilder.bytesType();
                break;
            default:
                throw new IllegalArgumentException("Unknown Avro primitive type: " + dataType);
        }
        return returnSchema;
    }

    protected static String getPrimitiveAvroTypeFromCassandraType(DataType dataType) {
        // Map types from Cassandra to Avro where possible
        if (dataType.equals(DataType.ascii())
                || dataType.equals(DataType.text())
                || dataType.equals(DataType.varchar())
                // Nonstandard types represented by this processor as a string
                || dataType.equals(DataType.timestamp())
                || dataType.equals(DataType.timeuuid())
                || dataType.equals(DataType.uuid())
                || dataType.equals(DataType.inet())
                || dataType.equals(DataType.varint())) {
            return "string";

        } else if (dataType.equals(DataType.cboolean())) {
            return "boolean";

        } else if (dataType.equals(DataType.cint())) {
            return "int";

        } else if (dataType.equals(DataType.bigint())
                || dataType.equals(DataType.counter())) {
            return "long";

        } else if (dataType.equals(DataType.cfloat())) {
            return "float";

        } else if (dataType.equals(DataType.cdouble())) {
            return "double";

        } else if (dataType.equals(DataType.blob())) {
            return "bytes";

        } else {
            throw new IllegalArgumentException("createSchema: Unknown Cassandra data type " + dataType.getName()
                    + " cannot be converted to Avro type");
        }
    }

    protected static DataType getPrimitiveDataTypeFromString(String dataTypeName) {
        Set<DataType> primitiveTypes = DataType.allPrimitiveTypes();
        for (DataType primitiveType : primitiveTypes) {
            if (primitiveType.toString().equals(dataTypeName)) {
                return primitiveType;
            }
        }
        return null;
    }

    /**
     * Gets a list of InetSocketAddress objects that correspond to host:port entries for Cassandra contact points
     *
     * @param contactPointList A comma-separated list of Cassandra contact points (host:port,host2:port2, etc.)
     * @return List of InetSocketAddresses for the Cassandra contact points
     */
    protected List<InetSocketAddress> getContactPoints(String contactPointList) {

        if (contactPointList == null) {
            return null;
        }
        final List<String> contactPointStringList = Arrays.asList(contactPointList.split(","));
        List<InetSocketAddress> contactPoints = new ArrayList<>();

        for (String contactPointEntry : contactPointStringList) {

            String[] addresses = contactPointEntry.split(":");
            final String hostName = addresses[0].trim();
            final int port = (addresses.length > 1) ? Integer.parseInt(addresses[1].trim()) : DEFAULT_CASSANDRA_PORT;

            contactPoints.add(new InetSocketAddress(hostName, port));
        }
        return contactPoints;
    }
}
