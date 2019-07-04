package org.apache.nifi.processors.ngsi;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.*;
import org.apache.nifi.processor.util.pattern.PartialFunctions.FlowFileGroup;
import org.apache.nifi.processors.ngsi.NGSI.backends.CartoBackend;
import org.apache.nifi.processors.ngsi.NGSI.utils.Entity;
import org.apache.nifi.processors.ngsi.NGSI.utils.NGSIEvent;
import org.apache.nifi.processors.ngsi.NGSI.utils.NGSIUtils;
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

//Aqui pondremos las propiedad
//Se puede ver que son las mismas propiedades que la pestaña de propiedades en la guia
public class NGSIToCarto extends AbstractSessionFactoryProcessor {
    public NGSIToCarto(){

    }
    static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool")
            .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
                    + "The Connection Pool is necessary in order to determine the appropriate database column types.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
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


    static final PropertyDescriptor NGSI_VERSION = new PropertyDescriptor.Builder()
            .name("ngsi-version")
            .displayName("NGSI Version")
            .description("The version of NGSI of your incomming events.")
            .required(false)
            .allowableValues("v2")
            .defaultValue("v2")
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

    static final PropertyDescriptor DEFAULT_SERVICE_PATH = new PropertyDescriptor.Builder()
            .name("default-service-path")
            .displayName("Default Service path")
            .description("Default Fiware ServicePath for building the table name")
            .required(false)
            .defaultValue("/path")
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

    static final PropertyDescriptor ENABLE_RAW_HISTORIC= new PropertyDescriptor.Builder()
            .name("enable-raw-historic")
            .displayName("Enable Raw Historic")
            .description("true or false, true applies raw-historic.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor ENABLE_DISTANCE_HISTORIC= new PropertyDescriptor.Builder()
            .name("enable-distance-historic")
            .displayName("Enable Distance")
            .description("true or false, true applies distance historic.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();


    static final PropertyDescriptor SWAP_COORDINATES= new PropertyDescriptor.Builder()
            .name("swap-coordinates")
            .displayName("Swap Coordinates")
            .description("true or false, true changing coordinates.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    static final PropertyDescriptor ENABLE_LOWERCASE= new PropertyDescriptor.Builder()
            .name("enable-lowercase")
            .displayName("Enable Lowercase")
            .description("true or false, true for creating the Schema and Tables name with lowercase.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor TRANSACTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Transaction Timeout")
            .description("If the <Support Fragmented Transactions> property is set to true, specifies how long to wait for all FlowFiles for a particular fragment.identifier attribute "
                    + "to arrive before just transferring all of the FlowFiles with that identifier to the 'failure' relationship")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();





    private static final String FRAGMENT_ID_ATTR = FragmentAttributes.FRAGMENT_ID.key();
    private static final String FRAGMENT_INDEX_ATTR = FragmentAttributes.FRAGMENT_INDEX.key();
    private static final String FRAGMENT_COUNT_ATTR = FragmentAttributes.FRAGMENT_COUNT.key();
    private static final CartoBackend carto = new CartoBackend();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_POOL);
        properties.add(NGSI_VERSION);
        properties.add(DATA_MODEL);
        properties.add(SWAP_COORDINATES);
        properties.add(DEFAULT_SERVICE);
        properties.add(DEFAULT_SERVICE_PATH);
        properties.add(ENABLE_ENCODING);
        properties.add(ENABLE_LOWERCASE);
        properties.add(BATCH_SIZE);
        properties.add(ENABLE_DISTANCE_HISTORIC);
        properties.add(ENABLE_RAW_HISTORIC);
        properties.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        return properties;
    }
    //Hacemos un HashSet con el tipo de relacion, si ha sido exitosa si ha fallado o si se vuelve a intentar
//Las variables usadas estan definidas mas arriba
//Haremos un HashSet de tipo relationship que son el tipo de variable que vamos a introducir
//Entra dentro de la configuracion del procesador
//Dentro de la configuracion se debera poder decir si quieres activar estas tres posibilidades de succes, etc
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

    private PutGroup<FunctionContext, Connection, StatementFlowFileEnclosure> process;
    private BiFunction<FunctionContext, ErrorTypes, ErrorTypes.Result> adjustError;
    private ExceptionHandler<FunctionContext> exceptionHandler;

   private final PartialFunctions.FetchFlowFiles<FunctionContext> fetchFlowFiles = (c, s, fc, r) -> {
        final FlowFilePoll poll = pollFlowFiles(c, s, fc, r);
        if (poll == null) {
            return null;
        }
        fc.fragmentedTransaction = poll.isFragmentedTransaction();
        return poll.getFlowFiles();
    };

    //Se establece una conexion al almacenamiento de datos
    private final PartialFunctions.InitConnection<FunctionContext, Connection> initConnection = (c, s, fc, ff) -> {
        final Connection connection = c.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class)
                .getConnection(ff == null ? Collections.emptyMap() : ff.getAttributes());
        try {
            fc.originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new ProcessException("Failed to disable auto commit due to " + e, e);
        }
        return connection;
    };

   @FunctionalInterface
    private interface GroupingFunction {
        void apply(final ProcessContext context, final ProcessSession session, final FunctionContext fc,
                   final Connection conn, final List<FlowFile> flowFiles,
                   final List<StatementFlowFileEnclosure> groups,
                   final Map<String, StatementFlowFileEnclosure> sqlToEnclosure,
                   final RoutingResult result);
    }
    //----------------------------------------------------------------------------------------------------------------------------------------------------------------------

    //Se crea el parámetro que puede ser leído por Carto a partir de los datos geográficos
    public final ImmutablePair<String, Boolean> getGeomWebmercator(String attrType, String attrValue, boolean SwapCoordiantes){
        ImmutablePair<String, Boolean> localizacion = new ImmutablePair<>("", true );


        if(attrType.equals("geo:point")){
            String[] split = attrValue.split(",");
            if(SwapCoordiantes){
                return new ImmutablePair("ST_SetSRID(ST_MakePoint(" + split[1].trim() /*+ "::float"*/ + "," + split[0].trim() /*+ "::float"*/ + "), 3857)", true);
            } else {
                return new ImmutablePair("ST_SetSRID(ST_MakePoint(" + split[0].trim() /*+ "::float"*/ + "," + split[1].trim() /*+ "::float"*/ + "), 3857)", true);
            }

        }

        else if(attrType.equals("geo:json")){
            return new ImmutablePair("ST_GeomFromGeoJSON(" + attrValue + ")", true);

        }
        return new ImmutablePair<>(attrValue, false);
    }

    //Se crea el parámetro que puede ser leído por Carto a partir de los datos geográficos
    public final ImmutablePair<String, Boolean> getGeom(String attrType, String attrValue, boolean SwapCoordiantes){

        ImmutablePair<String, Boolean> localizacion = new ImmutablePair<>("", true );


        if(attrType.equals("geo:point")){
            String[] split = attrValue.split(",");
            if(SwapCoordiantes){
                return new ImmutablePair("ST_SetSRID(ST_MakePoint(" + split[1].trim() /*+ "::float"*/ + "," + split[0].trim() /*+ "::float"*/ + "), 4326)", true);
            } else {
                return new ImmutablePair("ST_SetSRID(ST_MakePoint(" + split[0].trim() /*+ "::float"*/ + "," + split[1].trim() /*+ "::float"*/ + "), 4326)", true);
            }

        }

        else if(attrType.equals("geo:json")){
            return new ImmutablePair("ST_GeomFromGeoJSON(" + attrValue + ")", true);

        }
        return new ImmutablePair<>(attrValue, false);
    }

    public String getWiths(String persistence, String tableName, long creationTime, ImmutablePair imm, String schemaName){
        String withs="";
        if(persistence.compareToIgnoreCase("raw")==0){
            withs =  ""
                    + "WITH id AS"
                    + " ( SELECT (t.cartodb_id + 1) AS idf "
                    + " FROM (SELECT cartodb_id FROM " + schemaName + "." + tableName + " ORDER BY cartodb_id DESC LIMIT 1) AS t) ";

        } else if(persistence.compareToIgnoreCase("distance")==0){
            withs = ""
                    + "WITH id AS"
                    + " ( SELECT (t.cartodb_id + 1) AS idf "
                    + " FROM (SELECT cartodb_id FROM " + schemaName + "." + tableName + " ORDER BY cartodb_id DESC LIMIT 1) AS t) "
                    + ", calcs AS ("
                    + " SELECT cartodb_id, ST_DISTANCE(the_geom::geography,"+ imm.getLeft()+ ") AS stage_distance, "
                    + " ( " + creationTime + " - recvTimeTS) AS stage_time"
                    + "  FROM " + schemaName +"." + tableName + " ORDER BY cartodb_id DESC LIMIT 1), "
                    + " speed AS ("
                    +" SELECT (calcs.stage_distance / NULLIF(calcs.stage_time, 0)) AS stage_speed "
                    + " FROM calcs "
                    + " ), inserts AS ( "
                    + "   SELECT"
                    + "      (-1 * ((-1 * t1.sumDistance) - calcs.stage_distance)) AS sum_dist,"
                    + "      (-1 * ((-1 * t1.sumTime) - calcs.stage_time)) AS sum_time,"
                    + "      (-1 * ((-1 * t1.sumSpeed) - speed.stage_speed)) AS sum_speed,"
                    + "      (-1 * ((-1 * t1.sumDistance) - calcs.stage_distance)) "
                    + "          * (-1 * ((-1 * t1.sumDistance) - calcs.stage_distance)) AS sum2_dist,"
                    + "      (-1 * ((-1 * t1.sumTime) - calcs.stage_time)) "
                    + "          * (-1 * ((-1 * t1.sumTime) - calcs.stage_time)) AS sum2_time,"
                    + "      (-1 * ((-1 * t1.sumSpeed) - speed.stage_speed)) "
                    + "          * (-1 * ((-1 * t1.sumSpeed) - speed.stage_speed)) AS sum2_speed,"
                    + "      t1.max_distance,"
                    + "      t1.min_distance,"
                    + "      t1.max_time,"
                    + "      t1.min_time,"
                    + "      t1.max_speed,"
                    + "      t1.min_speed,"
                    + "      t2.num_samples"
                    + "   FROM"
                    + "      ("
                    + "         SELECT"
                    + "            GREATEST(calcs.stage_distance, maxDistance) AS max_distance,"
                    + "            LEAST(calcs.stage_distance, minDistance) AS min_distance,"
                    + "            GREATEST(calcs.stage_time, maxTime) AS max_time,"
                    + "            LEAST(calcs.stage_time, minTime) AS min_time,"
                    + "            GREATEST(speed.stage_speed, maxSpeed) AS max_speed,"
                    + "            LEAST(speed.stage_speed, minSpeed) AS min_speed,"
                    + "            sumDistance,"
                    + "            sumTime,"
                    + "            sumSpeed"
                    + " FROM " + schemaName + "." + tableName + ", speed, calcs"
                    +" ORDER BY " + schemaName +"." + tableName + "." + "cartodb_id DESC LIMIT 1) AS t1, "
                    + "      ("
                    + "         SELECT (-1 * ((-1 * COUNT(*)) - 1)) AS num_samples"
                    + "         FROM " + schemaName + "." + tableName
                    + "      ) AS t2,"
                    + "      speed,"
                    + "      calcs"
                    + ")";
        }
        return withs;
    }

    public String getRowCaseDistance(long creationTime, String servicePath, String entityId, String entityType, ImmutablePair imm, ImmutablePair imm2){
        String row ="( (SELECT idf FROM id)," + creationTime + ",'" + servicePath + "','" + entityId + "','" + entityType + "',"
                + imm.getLeft() + ","+ imm2.getLeft() + ",(SELECT stage_distance FROM calcs),"
                + "(SELECT stage_time FROM calcs),(SELECT stage_speed FROM speed),"
                + "(SELECT sum_dist FROM inserts),(SELECT sum_time FROM inserts),"
                + "(SELECT sum_speed FROM inserts),(SELECT sum2_dist FROM inserts),"
                + "(SELECT sum2_time FROM inserts),(SELECT sum2_speed FROM inserts),"
                + "(SELECT max_distance FROM inserts),(SELECT min_distance FROM inserts),"
                + "(SELECT max_time FROM inserts),(SELECT min_time FROM inserts),"
                + "(SELECT max_speed FROM inserts),(SELECT min_speed FROM inserts),"
                + "(SELECT num_samples FROM inserts))";
        return row;
    }

    private final GroupingFunction groupFlowFilesBySQLBatch = (context, session, fc, conn, flowFiles, groups, sqlToEnclosure, result) -> {
        for (final FlowFile flowFile : flowFiles) {

            String p = "";

            if(context.getProperty(ENABLE_RAW_HISTORIC).asBoolean()){
                p = "raw";

            } else if(context.getProperty(ENABLE_DISTANCE_HISTORIC).asBoolean()){
                p = "distance";
            }
            final String persistencia = p;


            final String fieldsDistance="(cartodb_id integer, recvTimeTs bigint, fiwareServicePath text, entityId text, entityType text, the_geom text, "
                    + "the_geom_webmercator text," + "stageDistance float, stageTime float, stageSpeed float, sumDistance float, "
                    + "sumTime float, sumSpeed float, sum2Distance float, sum2Time float, sum2Speed float, "
                    + "maxDistance float, minDistance float, maxTime float, minTime float, maxSpeed float, "
                    + "minSpeed float, numSamples bigint)";



            NGSIUtils n = new NGSIUtils();

            final NGSIEvent event=n.getEventFromFlowFile(flowFile,session,context.getProperty(NGSI_VERSION).getValue());

            final long creationTime = event.getCreationTime();
            final String fiwareService = (event.getFiwareService().compareToIgnoreCase("nd")==0)?context.getProperty(DEFAULT_SERVICE).getValue():event.getFiwareService();
            final String fiwareServicePath = (event.getFiwareServicePath().compareToIgnoreCase("/nd")==0)?context.getProperty(DEFAULT_SERVICE_PATH).getValue():event.getFiwareServicePath();


            try {

                final String schemaName = carto.buildSchemaName(fiwareService, context.getProperty(ENABLE_ENCODING).asBoolean(), context.getProperty(ENABLE_LOWERCASE).asBoolean());
                for (Entity entity : event.getEntities()) {
                    String sqlRaw = ";";
                    String sqlDistance = "";
                    String tableName1 = carto.buildTableName(fiwareServicePath, entity, context.getProperty(DATA_MODEL).getValue(), context.getProperty(ENABLE_ENCODING).asBoolean(), context.getProperty(ENABLE_LOWERCASE).asBoolean());

                    ImmutablePair<String, Boolean> geom = new ImmutablePair<>("", false);
                    ImmutablePair<String, Boolean> immWebmercator = new ImmutablePair<>("", false);


                    String nRows = "";
                    String attrName ="";
                    String attrType = "";
                    String withs = "";
                    String attrValue="";
                    final String withs2 = "";

                    for(int i=0; i<entity.getEntityAttrs().size();i++){

                        attrType = entity.getEntityAttrs().get(i).getAttrType();
                        attrName = entity.getEntityAttrs().get(i).getAttrName();
                        attrValue = entity.getEntityAttrs().get(i).getAttrValue();

                         if(attrType.compareToIgnoreCase("geo:point")==0 || attrType.compareToIgnoreCase("geo:json")==0){
                             ImmutablePair<String, Boolean> imm1 = new ImmutablePair<>("", true);
                             geom = imm1;
                         }
                         if(geom.getRight()) {
                             geom = getGeom(attrType, attrValue, context.getProperty(SWAP_COORDINATES).asBoolean());
                             immWebmercator = getGeomWebmercator(attrType, attrValue, context.getProperty(SWAP_COORDINATES).asBoolean());
                         }

                         boolean geo = geom.getRight();

                    }
                    //Se usa en caso de que la tabla este vacía, caso distance
                    final String rowsCasoDistanceVacia = "(1," + creationTime + ",'" + fiwareServicePath + "','" + entity.getEntityId() + "','" + entity.getEntityType() + "',"
                            + geom.getLeft() +","+ immWebmercator.getLeft() + ",0,0,0,0,0,0,0,0,0," + Float.MIN_VALUE + "," + Float.MAX_VALUE + ","
                            + Float.MIN_VALUE + "," + Float.MAX_VALUE + "," + Float.MIN_VALUE + "," + Float.MAX_VALUE
                            + ",1)";


                    if(persistencia.compareToIgnoreCase("raw")==0) {
                        withs = getWiths(persistencia, tableName1, creationTime, geom, schemaName);
                        //Sentencia para calcular el numero de filas de la tabla
                        nRows = carto.nRows(schemaName, tableName1);
                    }

                    else if(persistencia.compareToIgnoreCase("distance")==0){


                            withs= getWiths(persistencia, tableName1, creationTime, geom, schemaName);
                            String rows = getRowCaseDistance(creationTime, fiwareServicePath, entity.getEntityId(), entity.getEntityType(), geom, immWebmercator);
                            sqlDistance = carto.insertQuery(entity,creationTime,fiwareServicePath,schemaName,tableName1,persistencia,withs,geom,rows, immWebmercator, "");
                            //Sentencia para calcular el numero de filas de la tabla
                            nRows = carto.nRows(schemaName, tableName1);


                    }

                    //Creamos las variables finales a partir de las variables anteriores para poder ser utilizadas posteriomente
                    final String sql = carto.createSchema(schemaName);
                    final String sqlDistanceNoVaciaFinal = sqlDistance;
                    final String tableName = tableName1;
                    final ImmutablePair<String, Boolean> geomFinal = geom;
                    final String nRowsfinal = nRows;
                    final String sqlRawNoVaciaFinal= sqlRaw;
                    final ImmutablePair<String, Boolean> immMercator = immWebmercator;
                    final String withFinal = withs;


                    final StatementFlowFileEnclosure enclosure = sqlToEnclosure
                            .computeIfAbsent(sql, k -> {
                                final StatementFlowFileEnclosure newEnclosure = new StatementFlowFileEnclosure(sql);
                                groups.add(newEnclosure);
                                return newEnclosure;
                            });
                    if (!exceptionHandler.execute(fc, flowFile, input -> {

                        final PreparedStatement stmt = enclosure.getCachedStatement(conn);
                        JdbcCommon.setParameters(stmt, flowFile.getAttributes());
                        try {

                                conn.createStatement().execute(carto.createSchema(schemaName));
                                if(persistencia.compareToIgnoreCase("distance")==0){
                                    conn.createStatement().execute(carto.createTable(schemaName, tableName, persistencia, fieldsDistance, entity));
                                }else{
                                    conn.createStatement().execute(carto.createTable(schemaName, tableName, persistencia, "", entity ));
                                }
                                  if(persistencia.compareToIgnoreCase("raw")==0) {
                                    ResultSet set =  conn.createStatement().executeQuery(nRowsfinal);
                                    final int v = 1;
                                    final int miValor = (set.next())?set.getInt(1):v;
                                    if(miValor==0){
                                        conn.createStatement().execute(carto.insertQuery(entity, creationTime, fiwareServicePath, schemaName, tableName, persistencia, withFinal, geomFinal, "", immMercator, "enable"));
                                    }else {
                                        conn.createStatement().execute(carto.insertQuery(entity, creationTime, fiwareServicePath, schemaName, tableName, persistencia, withFinal, geomFinal, "", immMercator, ""));
                                    }
                                }

                                if(persistencia.compareToIgnoreCase("distance")==0) {
                                    //A continuación veremos si la tabla esta vacía o no
                                    ResultSet set =  conn.createStatement().executeQuery(nRowsfinal);
                                    final int v = 1;
                                    final int miValor = (set.next())?set.getInt(1):v;
                                    if (miValor == 0) {
                                        conn.createStatement().execute(carto.insertQuery(entity, creationTime, fiwareServicePath, schemaName, tableName, persistencia, withs2, geomFinal, rowsCasoDistanceVacia, immMercator, ""));
                                    } else {
                                        conn.createStatement().executeQuery(sqlDistanceNoVaciaFinal);
                                    }
                                }

                        } catch (SQLException s) {
                            getLogger().error(s.toString());
                        }
                        stmt.addBatch();
                    }, onFlowFileError(context, session, result))) {
                        continue;
                    }

                    enclosure.addFlowFile(flowFile);
                }
            }catch (Exception e){
                getLogger().error(e.toString());
            }
        }
    };

    private GroupingFunction groupFlowFilesBySQL = (context, session, fc, conn, flowFiles, groups, sqlToEnclosure, result) -> {

    };


    final PutGroup.GroupFlowFiles<FunctionContext, Connection, StatementFlowFileEnclosure> groupFlowFiles = (context, session, fc, conn, flowFiles, result) -> {
        final Map<String, StatementFlowFileEnclosure> sqlToEnclosure = new HashMap<>();
        final List<StatementFlowFileEnclosure> groups = new ArrayList<>();

        // There are three patterns:
        // 1. Support batching: An enclosure has multiple FlowFiles being executed in a batch operation
        // 2. Obtain keys: An enclosure has multiple FlowFiles, and each FlowFile is executed separately
        // 3. Fragmented transaction: One FlowFile per Enclosure?
        if (fc.obtainKeys) {
            groupFlowFilesBySQL.apply(context, session, fc, conn, flowFiles, groups, sqlToEnclosure, result);
        }

        else {
            groupFlowFilesBySQLBatch.apply(context, session, fc, conn, flowFiles, groups, sqlToEnclosure, result);
        }

        return groups;
    };
    /**
     * context - process context passed from a Processor onTrigger
     * session - process session passed from a Processor onTrigger
     * functionContext - function context passed from a Processor onTrigger
     * connection - connection to data storage, established by PartialFunctions.InitConnection
     * flowFiles - FlowFiles fetched from PartialFunctions.FetchFlowFiles
     * result - Route incoming FLowFiles if necessary

     */
    final PutGroup.PutFlowFiles<FunctionContext, Connection, StatementFlowFileEnclosure> putFlowFiles = (context, session, fc, conn, enclosure, result) -> {

        if (fc.isSupportBatching()) {

            // Tenemos PreparedStatement que tiene lotes agregados a ellos
            // Necesitamos ejecutar cada lote y cerrar PreparedStatement

            exceptionHandler.execute(fc, enclosure, input -> {
                try (final PreparedStatement stmt = enclosure.getCachedStatement(conn)) {
                    stmt.executeBatch();
                    result.routeTo(enclosure.getFlowFiles(), REL_SUCCESS);
                }
            }, onBatchUpdateError(context, session, result));

        } else {
            for (final FlowFile flowFile : enclosure.getFlowFiles()) {

                final StatementFlowFileEnclosure targetEnclosure
                        = enclosure instanceof FragmentedEnclosure
                        ? ((FragmentedEnclosure) enclosure).getTargetEnclosure(flowFile)
                        : enclosure;

                // Execute update one by one.
                exceptionHandler.execute(fc, flowFile, input -> {
                    try (final PreparedStatement stmt = targetEnclosure.getNewStatement(conn, fc.obtainKeys)) {

                        // set the appropriate parameters on the statement.
                        JdbcCommon.setParameters(stmt, flowFile.getAttributes());

                        stmt.executeUpdate();

                        // attempt to determine the key that was generated, if any. This is not supported by all
                        // database vendors, so if we cannot determine the generated key (or if the statement is not an INSERT),
                        // we will just move on without setting the attribute.
                        FlowFile sentFlowFile = flowFile;
                        final String generatedKey = determineGeneratedKey(stmt);
                        if (generatedKey != null) {
                            sentFlowFile = session.putAttribute(sentFlowFile, "sql.generated.key", generatedKey);
                        }

                        result.routeTo(sentFlowFile, REL_SUCCESS);

                    }
                }, onFlowFileError(context, session, result));
            }
        }

        if (result.contains(REL_SUCCESS)) {
            // Determine the database URL
            String url = "jdbc://unknown-host";
            try {
                url = conn.getMetaData().getURL();
            } catch (final SQLException sqle) {
            }

            // Emit a Provenance SEND event
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fc.startNanos);
            for (final FlowFile flowFile : result.getRoutedFlowFiles().get(REL_SUCCESS)) {
                session.getProvenanceReporter().send(flowFile, url, transmissionMillis, true);
            }
        }
    };

//Exception Handler proporciona una logica de manejo de excepciones estructurada compuesta por funcones parciales reutilizables
//Proporciona un programa mas limpio que solo se enfoca en la ruta esperada

//On error - Especifica una funcion OnError predeterminada que se llamara si no se especifica explicitamente cuando se llama a execute(Object, Object Procedure)

    private ExceptionHandler.OnError<FunctionContext, FlowFile> onFlowFileError(final ProcessContext context, final ProcessSession session, final RoutingResult result) {
        ExceptionHandler.OnError<FunctionContext, FlowFile> onFlowFileError = createOnError(context, session, result, REL_FAILURE, REL_RETRY);
        onFlowFileError = onFlowFileError.andThen((c, i, r, e) -> {
            switch (r.destination()) {
                case Failure:
                    getLogger().error("Failed to update database for {} due to {}; routing to failure", new Object[]{i, e}, e);
                    break;
                case Retry:
                    getLogger().error("Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry",
                            new Object[]{i, e}, e);
                    break;
            }
        });
        return RollbackOnFailure.createOnError(onFlowFileError);
    }

    private ExceptionHandler.OnError<FunctionContext, StatementFlowFileEnclosure> onBatchUpdateError(
            final ProcessContext context, final ProcessSession session, final RoutingResult result) {
        return RollbackOnFailure.createOnError((c, enclosure, r, e) -> {

            // If rollbackOnFailure is enabled, the error will be thrown as ProcessException instead.
            if (e instanceof BatchUpdateException && !c.isRollbackOnFailure()) {

                // If we get a BatchUpdateException, then we want to determine which FlowFile caused the failure,
                // and route that FlowFile to failure while routing those that finished processing to success and those
                // that have not yet been executed to retry.
                // Currently fragmented transaction does not use batch update.
                final int[] updateCounts = ((BatchUpdateException) e).getUpdateCounts();
                final List<FlowFile> batchFlowFiles = enclosure.getFlowFiles();

                // In the presence of a BatchUpdateException, the driver has the option of either stopping when an error
                // occurs, or continuing. If it continues, then it must account for all statements in the batch and for
                // those that fail return a Statement.EXECUTE_FAILED for the number of rows updated.
                // So we will iterate over all of the update counts returned. If any is equal to Statement.EXECUTE_FAILED,
                // we will route the corresponding FlowFile to failure. Otherwise, the FlowFile will go to success
                // unless it has not yet been processed (its index in the List > updateCounts.length).
                int failureCount = 0;
                int successCount = 0;
                int retryCount = 0;
                for (int i = 0; i < updateCounts.length; i++) {
                    final int updateCount = updateCounts[i];
                    final FlowFile flowFile = batchFlowFiles.get(i);
                    if (updateCount == Statement.EXECUTE_FAILED) {
                        result.routeTo(flowFile, REL_FAILURE);
                        failureCount++;
                    } else {
                        result.routeTo(flowFile, REL_SUCCESS);
                        successCount++;
                    }
                }

                if (failureCount == 0) {
                    // if no failures found, the driver decided not to execute the statements after the
                    // failure, so route the last one to failure.
                    final FlowFile failedFlowFile = batchFlowFiles.get(updateCounts.length);
                    result.routeTo(failedFlowFile, REL_FAILURE);
                    failureCount++;
                }

                if (updateCounts.length < batchFlowFiles.size()) {
                    final List<FlowFile> unexecuted = batchFlowFiles.subList(updateCounts.length + 1, batchFlowFiles.size());
                    for (final FlowFile flowFile : unexecuted) {
                        result.routeTo(flowFile, REL_RETRY);
                        retryCount++;
                    }
                }

                getLogger().error("Failed to update database due to a failed batch update, {}. There were a total of {} FlowFiles that failed, {} that succeeded, "
                        + "and {} that were not execute and will be routed to retry; ", new Object[]{e, failureCount, successCount, retryCount}, e);

                return;

            }

            // Apply default error handling and logging for other Exceptions.
            ExceptionHandler.OnError<RollbackOnFailure, FlowFileGroup> onGroupError
                    = ExceptionHandler.createOnGroupError(context, session, result, REL_FAILURE, REL_RETRY);
            onGroupError = onGroupError.andThen((cl, il, rl, el) -> {
                switch (r.destination()) {
                    case Failure:
                        getLogger().error("Failed to update database for {} due to {}; routing to failure", new Object[]{il.getFlowFiles(), e}, e);
                        break;
                    case Retry:
                        getLogger().error("Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry",
                                new Object[]{il.getFlowFiles(), e}, e);
                        break;
                }
            });
            onGroupError.apply(c, enclosure, r, e);
        });
    }


    @OnScheduled
    public void constructProcess() {
        process = new PutGroup<>();

        process.setLogger(getLogger());
        process.fetchFlowFiles(fetchFlowFiles);
        process.initConnection(initConnection); //Conexion con la base de datos Postgresql
        process.groupFetchedFlowFiles(groupFlowFiles);
        process.putFlowFiles(putFlowFiles);
        process.adjustRoute(RollbackOnFailure.createAdjustRoute(REL_FAILURE, REL_RETRY));

        process.onCompleted((c, s, fc, conn) -> {
            try {
                conn.commit();
            } catch (SQLException e) {
                // Throw ProcessException to rollback process session.
                throw new ProcessException("Failed to commit database connection due to " + e, e);
            }
        });

        process.onFailed((c, s, fc, conn, e) -> {
            try {
                conn.rollback();
            } catch (SQLException re) {
                // Just log the fact that rollback failed.
                // ProcessSession will be rollback by the thrown Exception so don't have to do anything here.
                getLogger().warn("Failed to rollback database connection due to %s", new Object[]{re}, re);
            }
        });

        process.cleanup((c, s, fc, conn) -> {
            // make sure that we try to set the auto commit back to whatever it was.
            if (fc.originalAutoCommit) {
                try {
                    conn.setAutoCommit(true);
                } catch (final SQLException se) {
                    getLogger().warn("Failed to reset autocommit due to {}", new Object[]{se});
                }
            }
        });

        exceptionHandler = new ExceptionHandler<>();
        exceptionHandler.mapException(e -> {
            if (e instanceof SQLNonTransientException) {
                return ErrorTypes.InvalidInput;
            } else if (e instanceof SQLException) {
                return ErrorTypes.TemporalFailure;
            } else {
                return ErrorTypes.UnknownFailure;
            }
        });
        adjustError = RollbackOnFailure.createAdjustError(getLogger());
        exceptionHandler.adjustError(adjustError);
    }



//Crea una sesion desde processSessionFactory y ejecuta la funcion onTRigger especificada
// y confirma la sesion si onTrigger finaliza correctamente. Cuando se lanza una excepcion durante
//la ejecucion del onTrigger, la sesion sera retrotaida. Los archivos en proceso que se procesen seran penalizados
//Se llamara al metodo onTrigger de un procesador cuando este rogramado para ejecutarse y cuando exista trabajo para el procesador.
//Hay trabajo si se cumple una de las siguientes condiciones:
// 1 : Una conexion cuyo destino es el procesador que tiene al menos un FlowFIle en cola
// 2 : Los procesadores no tienen conexiones entrantes
// 3 : El procesador esta anotado con la anotacion @TriggerWhenEmpty

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final Boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        final FunctionContext functionContext = new FunctionContext(rollbackOnFailure);
        functionContext.obtainKeys = false;
        RollbackOnFailure.onTrigger(context, sessionFactory, functionContext, getLogger(), session -> process.onTrigger(context, session, functionContext));
    }

    /**
     * Pulls a batch of FlowFiles from the incoming queues. If no FlowFiles are available, returns <code>null</code>.
     * Otherwise, a List of FlowFiles will be returned.
     * <p>
     * If all FlowFiles pulled are not eligible to be processed, the FlowFiles will be penalized and transferred back
     * to the input queue and an empty List will be returned.
     * <p>
     * Otherwise, if the Support Fragmented Transactions property is true, all FlowFiles that belong to the same
     * transaction will be sorted in the order that they should be evaluated.
     *
     * //@param context the process context for determining properties
     * //@param session the process session for pulling flowfiles
     * @return a FlowFilePoll containing a List of FlowFiles to process, or <code>null</code> if there are no FlowFiles to process
     */


    private FlowFilePoll pollFlowFiles(final ProcessContext context, final ProcessSession session,
                                       final FunctionContext functionContext, final RoutingResult result) {
        // Determine which FlowFile Filter to use in order to obtain FlowFiles.
        final boolean useTransactions = false;
        boolean fragmentedTransaction = false;

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles;
        if (useTransactions) {
            final TransactionalFlowFileFilter filter = new TransactionalFlowFileFilter();
            flowFiles = session.get(filter);
            fragmentedTransaction = filter.isFragmentedTransaction();
        } else {
            flowFiles = session.get(batchSize);
        }

        if (flowFiles.isEmpty()) {
            return null;
        }

        // If we are supporting fragmented transactions, verify that all FlowFiles are correct
        if (fragmentedTransaction) {
            try {
                if (!isFragmentedTransactionReady(flowFiles, context.getProperty(TRANSACTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS))) {
                    // Not ready, penalize FlowFiles and put it back to self.
                    flowFiles.forEach(f -> result.routeTo(session.penalize(f), Relationship.SELF));
                    return null;
                }

            } catch (IllegalArgumentException e) {
                // Map relationship based on context, and then let default handler to handle.
                final ErrorTypes.Result adjustedRoute = adjustError.apply(functionContext, ErrorTypes.InvalidInput);
                ExceptionHandler.createOnGroupError(context, session, result, REL_FAILURE, REL_RETRY)
                        .apply(functionContext, () -> flowFiles, adjustedRoute, e);
                return null;
            }

            // sort by fragment index.
            flowFiles.sort(Comparator.comparing(o -> Integer.parseInt(o.getAttribute(FRAGMENT_INDEX_ATTR))));
        }

        return new FlowFilePoll(flowFiles, fragmentedTransaction);
    }


    /**
     * Returns the key that was generated from the given statement, or <code>null</code> if no key
     * was generated or it could not be determined.
     *
     * @param stmt the statement that generated a key
     * @return the key that was generated from the given statement, or <code>null</code> if no key
     * was generated or it could not be determined.
     */

    private String determineGeneratedKey(final PreparedStatement stmt) {
        try {
            final ResultSet generatedKeys = stmt.getGeneratedKeys();
            if (generatedKeys != null && generatedKeys.next()) {
                return generatedKeys.getString(1);
            }
        } catch (final SQLException sqle) {
            // This is not supported by all vendors. This is a best-effort approach.
        }

        return null;
    }

    /**
     * Determines which relationship the given FlowFiles should go to, based on a transaction timing out or
     * transaction information not being present. If the FlowFiles should be processed and not transferred
     * to any particular relationship yet, will return <code>null</code>
     *
     * @param flowFiles the FlowFiles whose relationship is to be determined
     * @param transactionTimeoutMillis the maximum amount of time (in milliseconds) that we should wait
     *            for all FlowFiles in a transaction to be present before routing to failure
     * @return the appropriate relationship to route the FlowFiles to, or <code>null</code> if the FlowFiles
     *         should instead be processed
     */

    boolean isFragmentedTransactionReady(final List<FlowFile> flowFiles, final Long transactionTimeoutMillis) throws IllegalArgumentException {
        int selectedNumFragments = 0;
        final BitSet bitSet = new BitSet();

        BiFunction<String, Object[], IllegalArgumentException> illegal = (s, objects) -> new IllegalArgumentException(String.format(s, objects));

        for (final FlowFile flowFile : flowFiles) {
            final String fragmentCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTR);
            if (fragmentCount == null && flowFiles.size() == 1) {
                return true;
            } else if (fragmentCount == null) {
                throw illegal.apply("Cannot process %s because there are %d FlowFiles with the same fragment.identifier "
                        + "attribute but not all FlowFiles have a fragment.count attribute", new Object[] {flowFile, flowFiles.size()});
            }

            final int numFragments;
            try {
                numFragments = Integer.parseInt(fragmentCount);
            } catch (final NumberFormatException nfe) {
                throw illegal.apply("Cannot process %s because the fragment.count attribute has a value of '%s', which is not an integer",
                        new Object[] {flowFile, fragmentCount});
            }

            if (numFragments < 1) {
                throw illegal.apply("Cannot process %s because the fragment.count attribute has a value of '%s', which is not a positive integer",
                        new Object[] {flowFile, fragmentCount});
            }

            if (selectedNumFragments == 0) {
                selectedNumFragments = numFragments;
            } else if (numFragments != selectedNumFragments) {
                throw illegal.apply("Cannot process %s because the fragment.count attribute has different values for different FlowFiles with the same fragment.identifier",
                        new Object[] {flowFile});
            }

            final String fragmentIndex = flowFile.getAttribute(FRAGMENT_INDEX_ATTR);
            if (fragmentIndex == null) {
                throw illegal.apply("Cannot process %s because the fragment.index attribute is missing", new Object[] {flowFile});
            }

            final int idx;
            try {
                idx = Integer.parseInt(fragmentIndex);
            } catch (final NumberFormatException nfe) {
                throw illegal.apply("Cannot process %s because the fragment.index attribute has a value of '%s', which is not an integer",
                        new Object[] {flowFile, fragmentIndex});
            }

            if (idx < 0) {
                throw illegal.apply("Cannot process %s because the fragment.index attribute has a value of '%s', which is not a positive integer",
                        new Object[] {flowFile, fragmentIndex});
            }

            if (bitSet.get(idx)) {
                throw illegal.apply("Cannot process %s because it has the same value for the fragment.index attribute as another FlowFile with the same fragment.identifier",
                        new Object[] {flowFile});
            }

            bitSet.set(idx);
        }

        if (selectedNumFragments == flowFiles.size()) {
            return true; // no relationship to route FlowFiles to yet - process the FlowFiles.
        }

        long latestQueueTime = 0L;
        for (final FlowFile flowFile : flowFiles) {
            if (flowFile.getLastQueueDate() != null && flowFile.getLastQueueDate() > latestQueueTime) {
                latestQueueTime = flowFile.getLastQueueDate();
            }
        }

        if (transactionTimeoutMillis != null) {
            if (latestQueueTime > 0L && System.currentTimeMillis() - latestQueueTime > transactionTimeoutMillis) {
                throw illegal.apply("The transaction timeout has expired for the following FlowFiles; they will be routed to failure: %s", new Object[] {flowFiles});
            }
        }

        getLogger().debug("Not enough FlowFiles for transaction. Returning all FlowFiles to queue");
        return false;  // not enough FlowFiles for this transaction. Return them all to queue.
    }



    /**
     * A FlowFileFilter that is responsible for ensuring that the FlowFiles returned either belong
     * to the same "fragmented transaction" (i.e., 1 transaction whose information is fragmented
     * across multiple FlowFiles) or that none of the FlowFiles belongs to a fragmented transaction
     */

    static class TransactionalFlowFileFilter implements FlowFileFilter {
        private String selectedId = null;
        private int numSelected = 0;
        private boolean ignoreFragmentIdentifiers = false;

        public boolean isFragmentedTransaction() {
            return !ignoreFragmentIdentifiers;
        }

        @Override
        public FlowFileFilterResult filter(final FlowFile flowFile) {
            final String fragmentId = flowFile.getAttribute(FRAGMENT_ID_ATTR);
            final String fragCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTR);

            // if first FlowFile selected is not part of a fragmented transaction, then
            // we accept any FlowFile that is also not part of a fragmented transaction.
            if (ignoreFragmentIdentifiers) {
                if (fragmentId == null || "1".equals(fragCount)) {
                    return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                } else {
                    return FlowFileFilterResult.REJECT_AND_CONTINUE;
                }
            }

            if (fragmentId == null || "1".equals(fragCount)) {
                if (selectedId == null) {
                    // Only one FlowFile in the transaction.
                    ignoreFragmentIdentifiers = true;
                    return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                } else {
                    // we've already selected 1 FlowFile, and this one doesn't match.
                    return FlowFileFilterResult.REJECT_AND_CONTINUE;
                }
            }

            if (selectedId == null) {
                // select this fragment id as the chosen one.
                selectedId = fragmentId;
                numSelected++;
                return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
            }

            if (selectedId.equals(fragmentId)) {
                // fragment id's match. Find out if we have all of the necessary fragments or not.
                final int numFragments;
                if (fragCount != null && JdbcCommon.NUMBER_PATTERN.matcher(fragCount).matches()) {
                    numFragments = Integer.parseInt(fragCount);
                } else {
                    numFragments = Integer.MAX_VALUE;
                }

                if (numSelected >= numFragments - 1) {
                    // We have all of the fragments we need for this transaction.
                    return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                } else {
                    // We still need more fragments for this transaction, so accept this one and continue.
                    numSelected++;
                    return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                }
            } else {
                return FlowFileFilterResult.REJECT_AND_CONTINUE;
            }
        }
    }


    /**
     * Una estructura de datos simple e inmutable para contener una Lista de archivos de flujo y un indicador de si
     * o no esos FlowFiles representan una "transacción fragmentada", es decir, una colección de FlowFiles
     * que todo debe ejecutarse como una sola transacción (nos referimos a ella como una transacción fragmentada
     * porque la información para esa transacción, incluidos SQL y los parámetros, está fragmentada
     * a través de múltiples FlowFiles).
     */

    private static class FlowFilePoll {
        private final List<FlowFile> flowFiles;
        private final boolean fragmentedTransaction;

        public FlowFilePoll(final List<FlowFile> flowFiles, final boolean fragmentedTransaction) {
            this.flowFiles = flowFiles;
            this.fragmentedTransaction = fragmentedTransaction;
        }

        public List<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public boolean isFragmentedTransaction() {
            return fragmentedTransaction;
        }
    }


    private static class FragmentedEnclosure extends StatementFlowFileEnclosure {

        private final Map<FlowFile, StatementFlowFileEnclosure> flowFileToEnclosure = new HashMap<>();

        public FragmentedEnclosure() {
            super(null);
        }

        public void addFlowFile(final FlowFile flowFile, final StatementFlowFileEnclosure enclosure) {
            addFlowFile(flowFile);
            flowFileToEnclosure.put(flowFile, enclosure);
        }

        public StatementFlowFileEnclosure getTargetEnclosure(final FlowFile flowFile) {
            return flowFileToEnclosure.get(flowFile);
        }
    }

    /**
     * A simple, immutable data structure to hold a Prepared Statement and a List of FlowFiles
     * for which that statement should be evaluated.
     */

    private static class StatementFlowFileEnclosure implements FlowFileGroup {
        private final String sql;
        private PreparedStatement statement;
        private final List<FlowFile> flowFiles = new ArrayList<>();

        public StatementFlowFileEnclosure(String sql) {
            this.sql = sql;
        }

        public PreparedStatement getNewStatement(final Connection conn, final boolean obtainKeys) throws SQLException {
            if (obtainKeys) {
                // Create a new Prepared Statement, requesting that it return the generated keys.
                PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

                if (stmt == null) {
                    // since we are passing Statement.RETURN_GENERATED_KEYS, calls to conn.prepareStatement will
                    // in some cases (at least for DerbyDB) return null.
                    // We will attempt to recompile the statement without the generated keys being returned.
                    stmt = conn.prepareStatement(sql);
                }

                // If we need to obtain keys, then we cannot do a Batch Update. In this case,
                // we don't need to store the PreparedStatement in the Map because we aren't
                // doing an addBatch/executeBatch. Instead, we will use the statement once
                // and close it.
                return stmt;
            }

            return conn.prepareStatement(sql);
        }

        public PreparedStatement getCachedStatement(final Connection conn) throws SQLException {
            if (statement != null) {
                return statement;
            }

            statement = conn.prepareStatement(sql);
            return statement;
        }

        @Override
        public List<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public void addFlowFile(final FlowFile flowFile) {
            this.flowFiles.add(flowFile);
        }

        @Override
        public int hashCode() {
            return sql.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return false;
            }
            if (!(obj instanceof StatementFlowFileEnclosure)) {
                return false;
            }

            final StatementFlowFileEnclosure other = (StatementFlowFileEnclosure) obj;
            return sql.equals(other.sql);
        }
    }
}
