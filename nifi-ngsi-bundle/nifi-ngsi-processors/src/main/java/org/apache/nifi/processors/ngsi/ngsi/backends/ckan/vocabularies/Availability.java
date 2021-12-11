package org.apache.nifi.processors.ngsi.ngsi.backends.ckan.vocabularies;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

/**
 * DCAT-AP v2 distribution availability.
 * Taken from
 * https://raw.githubusercontent.com/SEMICeu/DCAT-AP/2.1.0-draft/releases/2.1.0/dcat-ap_2.1.0.rdf
 */

public class Availability {

    public static final String TEMPORARY = "http://data.europa.eu/r5r/availability/temporary";
    public static final String EXPERIMENTAL = "http://data.europa.eu/r5r/availability/experimental";
    public static final String AVAILABLE = "http://data.europa.eu/r5r/availability/available";
    public static final String STABLE = "http://data.europa.eu/r5r/availability/stable";

    /**
     * Constructor. It is private since utility classes should not have public or
     * default constructor
     */
    private Availability() {

    } // Availability

    public static Set<String> getAvailaibilityNames() {
        Set<String> availabilityNames = new TreeSet<>(Arrays.asList(TEMPORARY, EXPERIMENTAL, AVAILABLE, STABLE));
        return availabilityNames;
    } // getAvailabilityNames

} // Availability
