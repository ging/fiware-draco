package org.apache.nifi.processors.ngsi.errors;

public class DracoBadContextData extends DracoError {
    
    /**
     * Constructor.
     * @param dracoMessage
     */
    public DracoBadContextData(String dracoMessage) {
        super("BadContextData", null, dracoMessage, null);
    } // DracoBadContextData
    
    /**
     * Constructor.
     * @param dracoMessage
     * @param javaException
     * @param javaMessage
     */
    public DracoBadContextData(String dracoMessage, String javaException, String javaMessage) {
        super("BadContextData", javaException, dracoMessage, javaMessage);
    } // DracoBadContextData
    
} // DracoBadContextData
