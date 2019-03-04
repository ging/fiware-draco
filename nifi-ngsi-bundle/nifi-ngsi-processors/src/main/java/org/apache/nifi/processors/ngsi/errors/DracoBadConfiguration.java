package org.apache.nifi.processors.ngsi.errors;

public class DracoBadConfiguration extends DracoError {
    
    /**
     * Constructor.
     * @param dracoMessage
     */
    public DracoBadConfiguration(String dracoMessage) {
        super("DracoBadConfiguration", null, dracoMessage, null);
    } // CygnusBadConfiguration
    
    /**
     * Constructor.
     * @param dracoMessage
     * @param javaException
     * @param javaMessage
     */
    public DracoBadConfiguration(String dracoMessage, String javaException, String javaMessage) {
        super("DracoBadConfiguration", javaException, dracoMessage, javaMessage);
    } // DracoBadConfiguration
    
} // DracoBadConfiguration
