# Draco Roadmap

This product is a FIWARE Generic Enabler. If you would like to learn about the overall Roadmap of FIWARE, please check
section "Roadmap" on the FIWARE Catalogue.

## Introduction

This section elaborates on proposed new features or tasks which are expected to be added to the product in the
foreseeable future. There should be no assumption of a commitment to deliver these features on specific dates or in the
order given. The development team will be doing their best to follow the proposed dates and priorities, but please bear
in mind that plans to work on a given feature or task may be revised. All information is provided as a general guideline
only, and this section may be revised to provide newer information at any time.

## Short term

The following list of features are planned to be addressed in the short term, and incorporated in the next release of
the product:

-   **Quality Assurance** Include quality assurance for the Draco Generic enabler
-   **Resources FIWARE Academy**: Provide resources for training section in FIWARE academy
-   **NGSIToNeo4j Controller**: Controller for connecting Draco and Neo4j backend.
-   **NGSIToNeo4j Processor**: Processor for storing data coming from Orion Context Broker store NGSI-LD in a graph database. 
    system.
-   **NGSIToDynamoDB Processor**: Processor for storing data coming from Orion Context Broker in NGSI format to
    DynamoDB.
-   **NGSIToDynamoDB Controller**: Controller for connecting between Draco and DynamoDB backend.    

## Medium term

The following list of features are planned to be addressed in the medium term, typically within the subsequent
release(s) generated in the next 9 months after next planned release:

-   **Draco over kubernetes**: For fully deployments with UCON 
-   **NGSIToCKAN Controller**: Controller for connecting between Draco and CKAN backend.
-   **NGSIToCKAN Processor**: Processor for storing data coming from Orion Context Broker in NGSI format to CKAN.

## Long term

The following list of features are proposals regarding the longer-term evolution of the product even though development
of these features has not yet been scheduled for a release in the near future. Please feel free to contact us if you
wish to get involved in the implementation or influence the roadmap:

-   **NGSIToCouchDB Controller**: Controller for connecting between Draco and CouchDB backend.
-   **NGSIToCouchDB Processor**: Processor for storing data coming from Orion Context Broker in NGSI format to CouchDB.
-   **Caching and batching**: Add caching and batching mechanisms for optimizing the write to the different backends.
-   **New Data Structure for native NGSI-LD**: Planing to redefine the data models of the different backends for
    optimizing the storage of NGSI-LD data
-   **NGSIToCouchDB Controller**:ntegration planning with the Usage control framework for monitoring the sinks operations
