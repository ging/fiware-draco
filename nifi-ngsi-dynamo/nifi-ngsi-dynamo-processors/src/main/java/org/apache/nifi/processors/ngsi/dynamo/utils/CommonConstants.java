package org.apache.nifi.processors.ngsi.dynamo.utils;

public final class CommonConstants {

    // Maximum values
    public static final int MAX_CONNS           = 500;
    public static final int MAX_CONNS_PER_ROUTE = 100;

    // encoding
    public static final String INTERNAL_CONCATENATOR = "=";
    public static final String CONCATENATOR = "xffff";
    public static final String OLD_CONCATENATOR = "_";

    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private CommonConstants() {
    } // CommonConstants

} // CommonConstants
