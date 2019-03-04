package org.apache.nifi.processors.ngsi.errors;

public class DracoRuntimeError extends DracoError {
    
    /**
     * Constructor.
     * @param dracoMessage
     */
    public DracoRuntimeError(String dracoMessage) {
        super("DracoRuntimeError", null, dracoMessage, null);
    } // DracoRuntimeError
    
    /**
     * Constructor.
     * @param dracoMessage
     * @param javaException
     * @param javaMessage
     */
    public DracoRuntimeError(String dracoMessage, String javaException, String javaMessage) {
        super("DracoRuntimeError", javaException, dracoMessage, javaMessage);
    } // DracoRuntimeError
    
} // DracoRuntimeError
