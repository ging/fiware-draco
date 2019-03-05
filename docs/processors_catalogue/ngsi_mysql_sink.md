# NGSIToMySQL

## Functionality

`NGSIToMySQL`, is a processor designed to persist NGSI-like context data events within a
[MySQL server](https://www.mysql.com/). Usually, such a context data is notified by a
[Orion Context Broker](https://github.com/telefonicaid/fiware-orion) instance, but could be any other system speaking
the <i>NGSI language</i>.

Independently of the data generator, NGSI context data is always transformed into internal `NGSIEvent` objects at Draco
sources. In the end, the information within these events must be mapped into specific MySQL data structures.

Next sections will explain this in detail.

### <a name="section1.1"></a>Mapping NGSI events to `NGSIEvent` objects

Notified NGSI events (containing context data) are transformed into `NGSIEvent` objects (for each context element a
`NGSIEvent` is created; such an event is a mix of certain headers and a `ContextElement` object), independently of the
NGSI data generator or the final backend where it is persisted.

This is done at the Draco Listen HTTP Processor (also with the same name in native NiFi) received as FlowFile. This
processor map the flow file attributes and content into a NGSI event. Once translated, the data (now, as `NGSIEvent`
objects) is put into the internal channels for future consumption (see next section).

### <a name="section1.2"></a>Mapping `NGSIEvent`s to MySQL data structures

MySQL organizes the data in databases that contain tables of data rows. Such organization is exploited by `NGSIToMySQL`
each time a `NGSIEvent` is going to be persisted.

#### <a name="section1.2.1"></a>MySQL databases naming conventions

A database named as the notified `fiware-service` header value (or, in absence of such a header, the defaulted value for
the FIWARE service) is created (if not existing yet).

It must be said MySQL [only accepts](http://dev.mysql.com/doc/refman/5.7/en/identifiers.html) alphanumerics `$` and `_`.
This leads to certain [encoding](#section2.3.3) is applied depending on the `enable_encoding` configuration parameter.

MySQL [databases name length](http://dev.mysql.com/doc/refman/5.7/en/identifiers.html) is limited to 64 characters.

#### MySQL tables naming conventions

The name of these tables depends on the configured data model (see the [Configuration](#section2.1) section for more
details):

-   Data model by service path (`data_model=dm-by-service-path`). As the data model name denotes, the notified FIWARE
    service path (or the configured one as default in [`NGSIRestHandler`](./ngsi_rest_handler.md) is used as the name of
    the table. This allows the data about all the NGSI entities belonging to the same service path is stored in this
    unique table. The only constraint regarding this data model is the FIWARE service path cannot be the root one (`/`).
-   Data model by entity (`data_model=dm-by-entity`). For each entity, the notified/default FIWARE service path is
    concatenated to the notified entity ID and type in order to compose the table name. The concatenation character is
    `_` (underscore). If the FIWARE service path is the root one (`/`) then only the entity ID and type are
    concatenated.

It must be said MySQL [only accepts](http://dev.mysql.com/doc/refman/5.7/en/identifiers.html) alphanumerics `$` and `_`.
This leads to certain [encoding](#section2.3.5) is applied depending on the `enable_encoding` configuration parameter.

MySQL [tables name length](http://dev.mysql.com/doc/refman/5.7/en/identifiers.html) is limited to 64 characters.

The following table summarizes the table name composition (old encoding):

| FIWARE service path | `dm-by-service-path` | `dm-by-entity`                      |
| ------------------- | -------------------- | ----------------------------------- |
| `/`                 | N/A                  | `<entityId>_<entityType>`           |
| `/<svcPath>`        | `<svcPath>`          | `<svcPath>_<entityId>_<entityType>` |

The following table summarizes the table name composition (new encoding):

| FIWARE service path | `dm-by-service-path` | `dm-by-entity`                                   |
| ------------------- | -------------------- | ------------------------------------------------ |
| `/`                 | `x002f`              | `x002fxffff<entityId>xffff<entityType>`          |
| `/<svcPath>`        | `x002f<svcPath>`     | `x002f<svcPath>xffff<entityId>xffff<entityType>` |

Please observe the concatenation of entity ID and type is already given in the `notified_entities`/`grouped_entities`
header values (depending on using or not the grouping rules, see the [Configuration](#section2.1) section for more
details) within the `NGSIEvent`.

#### Row-like storing

Regarding the specific data stored within the above table, if `attr_persistence` parameter is set to `row` (default
storing mode) then the notified data is stored attribute by attribute, composing an insert for each one of them. Each
insert contains the following fields:

-   `recvTimeTs`: UTC timestamp expressed in miliseconds.
-   `recvTime`: UTC timestamp in human-readable format ([ISO 8601](http://en.wikipedia.org/wiki/ISO_8601)).
-   `fiwareServicePath`: Notified fiware-servicePath, or the default configured one if not notified.
-   `entityId`: Notified entity identifier.
-   `entityType`: Notified entity type.
-   `attrName`: Notified attribute name.
-   `attrType`: Notified attribute type.
-   `attrValue`: In its simplest form, this value is just a string, but since Orion 0.11.0 it can be Json object or Json
    array.
-   `attrMd`: It contains a string serialization of the metadata array for the attribute in Json (if the attribute
    hasn't metadata, an empty array `[]` is inserted).

#### Column-like storing

Regarding the specific data stored within the above table, if `attr_persistence` parameter is set to `column` then a
single line is composed for the whole notified entity, containing the following fields:

-   `recvTime`: Timestamp in human-readable format (Similar to [ISO 8601](http://en.wikipedia.org/wiki/ISO_8601), but
    avoiding the `Z` character denoting UTC, since all MySQL timestamps are supposed to be in UTC format).
-   `fiwareServicePath`: The notified one or the default one.
-   `entityId`: Notified entity identifier.
-   `entityType`: Notified entity type.
-   For each notified attribute, a field named as the attribute is considered. This field will store the attribute
    values along the time.
-   For each notified attribute, a field named as the concatenation of the attribute name and `_md` is considered. This
    field will store the attribute's metadata values along the time.

### Example

#### `NGSIEvent`

Assuming the following `NGSIEvent` is created from a notified NGSI context data (the code below is an <i>object
representation</i>, not any real data format):

```text
    ngsi-event={
        headers={
             content-type=application/json,
             timestamp=1429535775,
             transactionId=1429535775-308-0000000000,
             correlationId=1429535775-308-0000000000,
             fiware-service=vehicles,
             fiware-servicepath=/4wheels,
             <grouping_rules_interceptor_headers>,
             <name_mappings_interceptor_headers>
        },
        body={
            entityId=car1,
            entityType=car,
            attributes=[
                {
                    attrName=speed,
                    attrType=float,
                    attrValue=112.9
                },
                {
                    attrName=oil_level,
                    attrType=float,
                    attrValue=74.6
                }
            ]
        }
    }
```

#### Database and table names

The MySQL database name will always be `vehicles`.

The MySQL table names will be, depending on the configured data model, the following ones (old encoding):

| FIWARE service path | `dm-by-service-path` | `dm-by-entity`     |
| ------------------- | -------------------- | ------------------ |
| `/`                 | N/A                  | `car1_car`         |
| `/4wheels`          | `4wheels`            | `4wheels_car1_car` |

Using the new encoding:

| FIWARE service path | `dm-by-service-path` | `dm-by-entity`                  |
| ------------------- | -------------------- | ------------------------------- |
| `/`                 | `x002f`              | `car1xffffcar`                  |
| `/wheels`           | `x002f4wheels`       | `x002f4wheelsxffffcar1xffffcar` |

#### Row-like storing

Assuming `attr_persistence=row` as configuration parameter, then `NGSIToMySQL` will persist the data within the body as:

```bash
    mysql> select * from 4wheels_car1_car;
    +------------+----------------------------+-------------------+----------+------------+-------------+-----------+-----------+--------+
    | recvTimeTs | recvTime                   | fiwareServicePath | entityId | entityType | attrName    | attrType  | attrValue | attrMd |
    +------------+----------------------------+-------------------+----------+------------+-------------+-----------+-----------+--------+
    | 1429535775 | 2015-04-20T12:13:22.41.124 | 4wheels           | car1     | car        |  speed      | float     | 112.9     | []     |
    | 1429535775 | 2015-04-20T12:13:22.41.124 | 4wheels           | car1     | car        |  oil_level  | float     | 74.6      | []     |
    +------------+----------------------------+-------------------+----------+------------+-------------+-----------+-----------+--------+
    2 row in set (0.00 sec)
```

#### Column-like storing

If `attr_persistence=colum` then `NGSIToMySQL` will persist the data within the body as:

```bash
    mysql> select * from 4wheels_car1_car;
    +----------------------------+-------------------+----------+------------+-------+----------+-----------+--------------+
    | recvTime                   | fiwareServicePath | entityId | entityType | speed | speed_md | oil_level | oil_level_md |
    +----------------------------+-------------------+----------+------------+-------+----------+-----------+--------------+
    | 2015-04-20T12:13:22.41.124 | 4wheels           | car1     | car        | 112.9 | []       |  74.6     | []           |
    +----------------------------+-------------------+----------+------------+-------+----------+-----------+--------------+
    1 row in set (0.00 sec)
```

## Administration guide

### Configuration

`NGSIToMySQL` is configured through the following parameters(the names of required properties appear in bold)):

| Name                      | Default Value | Allowable Values | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------- | ------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **JDBC Connection Pool**  | no            |                  | Controller service for connecting to a specific database engine                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| **NGSI version**          | v2            |                  | list of supported version of NGSI (v2 and ld), currently only support v2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| **Data Model**            | db-by-entity  |                  | The Data model for creating the tables when an event have been received you can choose between: db-by-service-path or db-by-entity, default value is db-by-service-path                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| **Attribute persistence** | row           | row, column      | The mode of storing the data inside of the table allowable values are row and column                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Default Service           | test          |                  | In case you dont set the Fiware-Service header in the context broker, this value will be used as Fiware-Service                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Default Service path      | /path         |                  | In case you dont set the Fiware-ServicePath header in the context broker, this value will be used as Fiware-ServicePath                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Enable encoding           | true          | true, false      | true applies the new encoding, false applies the old encoding.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Enable lowercase          | true          | true, false      | true for creating the Schema and Tables name with lowercase.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| **Batch size**            | 10            |                  | The preferred number of FlowFiles to put to the database in a single transaction                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| **Rollback on failure**   | false         | true, false      | Specify how to handle error. By default (false), if an error occurs while processing a FlowFile, the FlowFile will be routed to 'failure' or 'retry' relationship based on error type, and processor can continue with next FlowFile. Instead, you may want to rollback currently processed FlowFiles and stop further processing immediately. In that case, you can do so by enabling this 'Rollback On Failure' property. If enabled, failed FlowFiles will stay in the input relationship without penalizing it and being processed repeatedly until it gets processed successfully or removed by other means. It is important to set adequate 'Yield Duration' to avoid retrying too frequently. |

A configuration example could be:

![mysql-processor](../images/processor-mysql.png)

### Use cases

Use `NGSIToMySQL` if you are looking for a database storage not growing so much in the mid-long term.

### Important notes

#### About the table

The table type configuration parameter, as seen, is a method for <i>direct</i> aggregation of data: by <i>default</i>
destination (i.e. all the notifications about the same entity will be stored within the same MySQL table) or by
<i>default</i> service-path (i.e. all the notifications about the same service-path will be stored within the same MySQL
table).

#### About the persistence mode

Please observe not always the same number of attributes is notified; this depends on the subscription made to the
NGSI-like sender. This is not a problem for the `row` persistence mode, since fixed 8-fields data rows are inserted for
each notified attribute. Nevertheless, the `column` mode may be affected by several data rows of different lengths (in
term of fields). Thus, the `column` mode is only recommended if your subscription is designed for always sending the
same attributes, event if they were not updated since the last notification.

In addition, when running in `column` mode, due to the number of notified attributes (and therefore the number of fields
to be written within the Datastore) is unknown by Draco, the table can not be automatically created, and must be
provisioned previously to the Draco execution. That's not the case of the `row` mode since the number of fields to be
written is always constant, independently of the number of notified attributes.

#### About batching

As explained in the [programmers guide](#section3), `NGSIToMySQL` extends `NGSISink`, which provides a built-in
mechanism for collecting events from the internal Flume channel. This mechanism allows extending classes have only to
deal with the persistence details of such a batch of events in the final backend.

What is important regarding the batch mechanism is it largely increases the performance of the sink, because the number
of writes is dramatically reduced. Let's see an example, let's assume a batch of 100 `NGSIEvent`s. In the best case, all
these events regard to the same entity, which means all the data within them will be persisted in the same MySQL table.
If processing the events one by one, we would need 100 inserts into MySQL; nevertheless, in this example only one insert
is required. Obviously, not all the events will always regard to the same unique entity, and many entities may be
involved within a batch. But that's not a problem, since several sub-batches of events are created within a batch, one
sub-batch per final destination MySQL table. In the worst case, the whole 100 entities will be about 100 different
entities (100 different MySQL tables), but that will not be the usual scenario. Thus, assuming a realistic number of
10-15 sub-batches per batch, we are replacing the 100 inserts of the event by event approach with only 10-15 inserts.

The batch mechanism adds an accumulation timeout to prevent the sink stays in an eternal state of batch building when no
new data arrives. If such a timeout is reached, then the batch is persisted as it is.

Regarding the retries of not persisted batches, a couple of parameters is used. On the one hand, a Time-To-Live (TTL) is
used, specifing the number of retries Draco will do before definitely dropping the event. On the other hand, a list of
retry intervals can be configured. Such a list defines the first retry interval, then se second retry interval, and so
on; if the TTL is greater than the length of the list, then the last retry interval is repeated as many times as
necessary.

By default, `NGSIToMySQL` has a configured batch size and batch accumulation timeout of 1 and 30 seconds, respectively.
Nevertheless, as explained above, it is highly recommended to increase at least the batch size for performance purposes.
Which are the optimal values? The size of the batch it is closely related to the transaction size of the channel the
events are got from (it has no sense the first one is greater then the second one), and it depends on the number of
estimated sub-batches as well. The accumulation timeout will depend on how often you want to see new data in the final
storage.

#### Time zone information

Time zone information is not added in MySQL timestamps since MySQL stores that information as a environment variable.
MySQL timestamps are stored in UTC time.

#### About the encoding

Until version 1.2.0 (included), Draco applied a very simple encoding:

-   All non alphanumeric characters were replaced by underscore, `_`.
-   The underscore was used as concatenator character as well.
-   The slash, `/`, in the FIWARE service paths is ignored.

From version 1.3.0 (included), Draco applies this specific encoding tailored to MySQL data structures:

-   Alphanumeric characters are not encoded.
-   Numeric characters are not encoded.
-   Underscore character, `_`, is not encoded.
-   Equals character, `=`, is encoded as `xffff`.
-   All other characters, including the slash in the FIWARE service paths, are encoded as a `x` character followed by
    the [Unicode](http://unicode-table.com) of the character.
-   User defined strings composed of a `x` character and a Unicode are encoded as `xx` followed by the Unicode.
-   All the other characters are not encoded.
-   `xffff` is used as concatenator character.

Despite the old encoding will be deprecated in the future, it is possible to switch the encoding type through the
`enable_encoding` parameter as explained in the [configuration](#section2.1) section.

#### About capping resources and expirating records

Capping and expiration are disabled by default. Nevertheless, if desired, this can be enabled:

-   Capping by the number of records. This allows the resource growing up until certain configured maximum number of
    records is reached (`persistence_policy.max_records`), and then maintains such a constant number of records.
-   Expirating by time the records. This allows the resource growing up until records become old, i.e. exceed certain
    configured expiration time (`persistence_policy.expiration_time`).
