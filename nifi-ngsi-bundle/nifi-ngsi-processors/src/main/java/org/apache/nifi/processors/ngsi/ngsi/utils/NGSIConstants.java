package org.apache.nifi.processors.ngsi.ngsi.utils;

public final class NGSIConstants {

    // Common fields for sinks
    public static final String RECV_TIME_TS        = "recvTimeTs";
    public static final String RECV_TIME           = "recvTime";
    public static final String FIWARE_SERVICE_PATH = "fiwareServicePath";
    public static final String ENTITY_ID           = "entityId";
    public static final String ENTITY_TYPE         = "entityType";
    public static final String ATTR_NAME           = "attrName";
    public static final String ATTR_TYPE           = "attrType";
    public static final String ATTR_VALUE          = "attrValue";
    public static final String ATTR_MD             = "attrMd";
    public static final String ATTR_MD_FILE        = "attrMdFile";

    // FIWARE service and FIWARE service path specific constants
    public static final int SERVICE_HEADER_MAX_LEN      = 50;
    public static final int SERVICE_PATH_HEADER_MAX_LEN = 50;

    // NGSIRestHandler specific constants
    public static final String PARAM_DEFAULT_SERVICE      = "default_service";
    public static final String PARAM_DEFAULT_SERVICE_PATH = "default_service_path";
    public static final String PARAM_NOTIFICATION_TARGET  = "notification_target";

    //NGSICKANSink specific constants
    // http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.organization_create
    // http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.package_create
    public static final int CKAN_MAX_NAME_LEN = 100;
    public static final int CKAN_MIN_NAME_LEN = 2;

    // NGSICartoDBSink specific constants
    // http://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    public static final int    CARTO_DB_MAX_NAME_LEN = 63;
    public static final String CARTO_DB_THE_GEOM = "the_geom";

    // NGSIDynamoDBSink specific constants
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-naming-rules
    public static final int    DYNAMO_DB_MIN_NAME_LEN = 3;
    public static final int    DYNAMO_DB_MAX_NAME_LEN = 255;
    public static final String DYNAMO_DB_PRIMARY_KEY  = "ID";

    // NGSIHDFSSink specific constants
    public static final int HDFS_MAX_NAME_LEN = 255;

    // NGSIMongoSink/NGSISTHSink specific constants
    // https://docs.mongodb.com/manual/reference/limits/#naming-restrictions
    public static final int MONGO_DB_MAX_NAMESPACE_SIZE_IN_BYTES = 113;
    public static final int MONGO_DB_MIN_HASH_SIZE_IN_BYTES      = 20;

    //NGSIMySQLSink specific constants
    // http://dev.mysql.com/doc/refman/5.7/en/identifiers.html
    public static final int MYSQL_MAX_NAME_LEN = 64;

    // NGSIPostgreSQLSink specific constants
    // http://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    public static final int POSTGRESQL_MAX_NAME_LEN = 63;

    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private NGSIConstants() {
    } // NGSIConstants


} // NGSIConstants