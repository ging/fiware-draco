package org.apache.nifi.processors.ngsi.NGSI.utils;

public final class CommonConstants {

    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private CommonConstants() {
    } // CommonConstants

    // Not applicable
    public static final String NA = "N/A";

    // Http header names
    public static final String HTTP_HEADER_CONTENT_TYPE = "content-type";

    // Maximum values
    public static final int MAX_CONNS           = 500;
    public static final int MAX_CONNS_PER_ROUTE = 100;

    // Http headers probably used by Flume events as well... TBD: should not be here!!
    public static final String HEADER_FIWARE_SERVICE      = "fiware-service";
    public static final String HEADER_FIWARE_SERVICE_PATH = "fiware-servicepath";
    public static final String HEADER_CORRELATOR_ID       = "fiware-correlator";

    // Used by CKANBackendImpl... TBD: should not be here!!
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

    // Others
    public static final String EMPTY_MD = "[]";

    // log4j specific constants
    public static final String LOG4J_CORR = "correlatorId";
    public static final String LOG4J_TRANS = "transactionId";
    public static final String LOG4J_SVC = "service";
    public static final String LOG4J_SUBSVC = "subservice";
    public static final String LOG4J_COMP = "agent";

    // encoding
    public static final String INTERNAL_CONCATENATOR = "=";
    public static final String CONCATENATOR = "xffff";
    public static final String OLD_CONCATENATOR = "_";

    // Header for API uses
    public static final String Draco_IPR_HEADER = "#####\n";
    // Enumeration for ManagementInterface
    public enum LoggingLevels { FATAL, ERROR, WARN, INFO, DEBUG, ALL, OFF }

} // CommonConstants
