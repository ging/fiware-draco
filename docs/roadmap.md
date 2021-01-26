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

-   **New Data Structure for native NGSI-LD**: Planing to redefine the data models of the different backends for
    optimizing the storage of NGSI-LD data
-   **NGSILD Support for NGSIToPostgreSQL**:  Receive and store context information in PostgreSQL using NGSI-LD notifications. 
-   **NGSILD Support for NGSIToCKAN**:  Receive and store context information in CKAN using NGSI-LD notifications.

## Medium term

The following list of features are planned to be addressed in the medium term, typically within the subsequent
release(s) generated in the next 9 months after next planned release:

-   **Quality Assurance** Include quality assurance for the Draco Generic enabler
-   **DCAT-AP compatibility with CKAN**: Managing DCAT-AP metadata for data publication in European Data Portal.
-   **Aeronautics Templates**: New templates for providing conversion to Aeronautics data models.
-   **NGSI-LD integration PostGIS**: Receive and store context information in PostGID using NGSI-LD notifications.
-   **NGSIToCouchDB Controller**: Controller for connecting between Draco and CouchDB backend.
-   **NGSIToCouchDB Processor**: Processor for storing data coming from Orion Context Broker in NGSI format to CouchDB.

## Long term

The following list of features are proposals regarding the longer-term evolution of the product even though development
of these features has not yet been scheduled for a release in the near future. Please feel free to contact us if you
wish to get involved in the implementation or influence the roadmap:

-   **UCON Integration**:  Integration planning with the Usage control framework for monitoring the sinks operations.
-   **Draco over kubernetes:**: For fully deployments with UCON.
-   **Blockchain integration**: Hyperledger Fabric integration : high performance. Ocean Protocol and Ethereum experimental.
-   **Graph oriented Databases**: Add new processors and controllers for supporting graph oriented databases like neo4j
    and RDF databases.
-   **NGSIToNeo Processor**: Processor for storing data in NGSI-LD format to Neo4j.
-   **Caching and batching**: Add caching and batching mechanisms for optimizing the write to the different backends.
