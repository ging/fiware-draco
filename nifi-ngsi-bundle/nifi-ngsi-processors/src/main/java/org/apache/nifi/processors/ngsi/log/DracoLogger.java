package org.apache.nifi.processors.ngsi.log;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DracoLogger {
    
    private static String fqcn = DracoLogger.class.getName();
    private Logger logger;
    
    /**
     * Constructor.
     * @param name
     */
    public DracoLogger(String name) {
        this.logger = Logger.getLogger(name);
    } // CygnusLogger
    
    /**
     * Constructor.
     * @param clazz
     */
    public DracoLogger(Class clazz) {
        this(clazz.getName());
    } // DracoLogger
    
    /**
     * Traces a message with FATAL level.
     * @param msg
     */
    public void fatal(Object msg) {
        try {
            logger.log(fqcn, Level.FATAL, msg, null);
        } catch (Exception e) {
            traceAndExit(e);
        } // try catch
    } // fatal
    
    /**
     * Traces a message with ERROR level.
     * @param msg
     */
    public void error(Object msg) {
        try {
            logger.log(fqcn, Level.ERROR, msg, null);
        } catch (Exception e) {
            traceAndExit(e);
        } // try catch
    } // error
    
    /**
     * Traces a message with DEBUG level.
     * @param msg
     */
    public void debug(Object msg) {
        try {
            logger.log(fqcn, Level.DEBUG, msg, null);
        } catch (Exception e) {
            traceAndExit(e);
        } // try catch
    } // debug
    
    /**
     * Traces a message with INFO level.
     * @param msg
     */
    public void info(Object msg) {
        try {
            logger.log(fqcn, Level.INFO, msg, null);
        } catch (Exception e) {
            traceAndExit(e);
        } // try catch
    } // info
    
    /**
     * Traces a message with WARN level.
     * @param msg
     */
    public void warn(Object msg) {
        try {
            logger.log(fqcn, Level.WARN, msg, null);
        } catch (Exception e) {
            traceAndExit(e);
        } // try catch
    } // warn
    
    private void traceAndExit(Exception e) {
        System.err.println("A problem with the logging system was found... shutting down Draco right now!"
                + " Details=" + e.getMessage());
        System.exit(-1);
    } // traceAndExit
    
} // DracoLogger
