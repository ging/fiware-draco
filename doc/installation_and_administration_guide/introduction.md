# <a name="top"></a>Introduction
This document details how to install and administrate a Draco with all the NGSI processors and resources.

Draco is composed by a set of processors in charge of persisting Orion context data in third-party storages, allowing to create a historical view of such data. In other words, Orion only stores the last value regarding an entity's attribute, and if an older value is required, then you will have to persist it in other storage, value by value, using Draco.

Draco uses the subscription/notification feature of Orion. A subscription is made in Orion on behalf of Draco-ngsi, detailing which entities we want to be notified when an update occurs on any of those entities attributes.

### Terminology

In order to talk about Draco, there are a few key terms that readers should be familiar with. 
Internally, Draco is based on [Apache NIFI](https://nifi.apache.org/), so along of this document, we will use the name NiFi and Draco as synonyms.
 
Now, we will explain those NiFi-specific terms here, at a high level.

![Architecture-Draco](../images/architecture-nifi.png)

**FlowFile:** Each piece of "User Data" (i.e., data that the user brings into NiFi for processing and distribution) is referred to as a FlowFile. A FlowFile is made up of two parts: Attributes and Content. The Content is the User Data itself. Attributes are key-value pairs that are associated with the User Data.

**Processor:** The Processor is the NiFi component that is responsible for creating, sending, receiving, transforming, routing, splitting, merging, and processing FlowFiles. It is the most important building block available to NiFi users to build their dataflows.

**Relationship**: Each Processor has zero or more Relationships defined for it. These Relationships are named to indicate the result of processing a FlowFile. After a Processor has finished processing a FlowFile, it will route (or “transfer”) the FlowFile to one of the Relationships. A DFM is then able to connect each of these Relationships to other components in order to specify where the FlowFile should go next under each potential processing result.

**Connection:** A DFM creates an automated dataflow by dragging components from the Components part of the NiFi toolbar to the canvas and then connecting the components together via Connections. Each connection consists of one or more Relationships. For each Connection that is drawn, a DFM can determine which Relationships should be used for the Connection. This allows data to be routed in different ways based on its processing outcome. Each connection houses a FlowFile Queue. When a FlowFile is transferred to a particular Relationship, it is added to the queue belonging to the associated Connection.

**Controller Service:** Controller Services are extension points that, after being added and configured by a DFM in the User Interface, will start up when NiFi starts up and provide information for use by other components (such as processors or other controller services). A common Controller Service used by several components is the StandardSSLContextService. It provides the ability to configure keystore and/or truststore properties once and reuse that configuration throughout the application. The idea is that, rather than configure this information in every processor that might need it, the controller service provides it for any processor to use as needed.


Draco is designed to run a specific set of processors and templates for 
storing context data to multiple sinks.


Actually, Draco use the engine of knife and adding new processors and templates. Basically, a source processor is in charge of receiving the data and convert it into a Flow Files. Afterward, they are transferred to other processors, which transform the Flow Files into NGSI Events for finally persisting the data into a third-party storage.

Current stable release of Draco is able to persist Orion context data in:

* [MySQL](https://www.mysql.com/), the well-know relational database manager.
* [MongoDB](https://www.mongodb.org/), the NoSQL document-oriented database.
* [PostgreSQL](http://www.postgresql.org/), the well-known relational database manager.


[Top](#top)

## Intended audience
This document is mainly addressed to those FIWARE users already using an [Orion Context Broker](https://github.com/telefonicaid/fiware-orion) instance and willing to create historical views from the context data managed by Orion. In that case, you will need this document in order to learn how to install and administrate Draco.

If your aim is to create a new processors for Draco or NiFi , or expand it in some way, please refer to the [User and Programmer Guide]().

[Top](#top)

## Structure of the document
Apart from this introduction, this Installation and Administration Guide mainly contains sections about installing, configuring, running and testing Draco. 
The FIWARE user will also find useful information regarding multitenancy or performance tips. In addition, sanity check procedures (useful to know wether the installation was successful or not) and diagnosis procedures (a set of tips aiming to help when an issue arises) are provided as well.

It is very important to note that, for those topics not covered by this documentation, the official documentation of 
[APACHE NiFI](https://nifi.apache.org/docs.html) provides a full and deeper explanation of all the services that NiFi provides. 

[Top](#top)
