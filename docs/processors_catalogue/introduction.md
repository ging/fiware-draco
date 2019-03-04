# Processors catalogue

This document details the catalogue of processors developed for Draco on top of [Apache NiFi](https://nifi.apache.org/).

# Intended audience

The Draco extensions catalogue is a basic piece of documentation for all those FIWARE users using Draco. It describes
the available extra components added to the NiFi technology in order to deal with NGSI-like context data in terms of
historic building.

Software developers may also be interested in this catalogue since it may guide the creation of new components
(specially, processors) for Draco/NiFi.

# Structure of the document

The document starts detailing the naming conventions adopted in Draco when creating data structures in the different
storages. This means those data structure (databases, files, tables, collections, etc) names will derive from a subset
of the NGSI-like notified information (mainly fiware-service and fiware-servicePath headers, entityId and entityType).

Then, it is time to explain `NGSIRestHandler` the NGSI oriented handler for the http NiFI source in charge of
translating a NGSI-like notification into a Flow File.

Then, each one of the NGSI oriented processors are described; for each processor an explanation about the functionality
(including how the information within a NGSI event is mapped into the storage data structures, according to the above
mentioned naming conventions), configuration, uses cases and implementation details are given.
