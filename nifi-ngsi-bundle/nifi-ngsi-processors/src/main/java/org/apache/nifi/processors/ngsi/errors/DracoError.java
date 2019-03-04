package org.apache.nifi.processors.ngsi.errors;

public class DracoError extends Exception {
    
    /**
     * Constructor.
     * @param dracocygnusException
     * @param javaException
     * @param dracoMessage
     * @param javaMessage
     */
    public DracoError(String dracoException, String javaException, String dracoMessage, String javaMessage) {
        super(dracoException + (javaException == null ? ". " : " (" + javaException + "). ")
                + dracoMessage + (javaMessage == null ? ". " : " (" + javaMessage + "). "));
    } // DracoError
    
} // DracoError
