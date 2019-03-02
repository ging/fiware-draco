### Templates

Draco includes some templates in order to reduce the configuration complexity for many processors and controllers. . A
Template is a way of combining these basic building blocks into larger building blocks. Once a DataFlow has been
created, parts of it can be formed into a Template. This Template can then be dragged onto the canvas, or can be
exported as an XML file and shared with others. Templates received from others can then be imported into an instance of
NiFi and dragged into the canvas.

The templates provided in Draco are:

-   Orion-To-MySQL
-   Orion-To-PostgreSQL
-   Orion-TO-Mongo

You can access each template by dragging the Template button to the canvas in the Draco user interface. Once you select
the template that you need, all the processors and controllers with a basic configuration will be put and displayed on
the Draco canvas.

![Draco-template1](../images/draco-template1.png)

### Orion-To-MySQL

The Orion-To-Mysql template includes some preconfigured processors and controllers:

-   DBCPConnectionPool controller for setting up the connection between the processors and the MySQL database.
-   ListenHTTP processor for receiving data from the Orion Context Broker.
-   NGSIToMySQL processor for creating updating and ingesting data to MYSQL
-   Log Attribute processor for keeping a register of the notifications received in Draco.

Now you can go to the properties tab of each processor and edit their properties according to your needs. Once you have
set up your processors, please enable the DBCPConnectionPool controller as is explained in the
[Draco GUI](./Draco_gui.md) and start the processors. For a more guided example, please go to the
[Quick Start Guide](../quick_start_guide.md)

### Orion-To-PostgreSQL

The Orion-To-PostgreSQL template includes some preconfigured processors and controllers:

-   DBCPConnectionPool controller for setting up the connection between the processors and the PostgreSQL database.
-   ListenHTTP processor for receiving data from the Orion Context Broker.
-   NGSIToPostgreSQL processor for creating updating and ingesting data to PostgreSQL
-   Log Attribute processor for keeping a register of the notifications received in Draco.

Now you can go to the properties tab of each processor and edit their properties according to your needs. Once you have
set up your processors, please enable the DBCPConnectionPool controller as is explained in the
[Draco GUI](./Draco_gui.md) and start the processors. For a more guided example, please go to the
[Quick Start Guide](../quick_start_guide.md)

### Orion-To-Mongo

The Orion-To-Mongo template includes some preconfigured processors and controllers:

-   ListenHTTP processor for receiving data from the Orion Context Broker.
-   NGSIToMySQL processor for creating documents, collections, updating and inserting data to Mongo
-   Log Attribute processor for keeping a register of the notifications received in Draco.

Now you can go to the properties tab of each processor and edit their properties according to your needs. Once you have
set up your processors, please start the processors. For a more guided example, please go to the
[Quick Start Guide](../quick_start_guide.md)
