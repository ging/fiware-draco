package org.apache.nifi.processors.ngsi.errors;

public class DracoPersistenceError extends DracoError {
    
    /**
     * Constructor.
     * @param dracoMessage
     */
    public DracoPersistenceError(String dracoMessage) {
        super("DracoPersistenceError", null, dracoMessage, null);
    } // CygnusPersistenceError
    
    /**
     * Constructor.
     * @param dracoMessage
     * @param javaException
     * @param javaMessage
     */
    public DracoPersistenceError(String dracoMessage, String javaException, String javaMessage) {
        super("DracoPersistenceError", javaException, dracoMessage, javaMessage);
    } // DracoPersistenceError
    
} // DracoPersistenceError
