# <a name="top"></a>ListenHTTP (NGSIRestHandler)

Content:

-   [Functionality](#section1)
    -   [Mapping NGSI events to `NGSIEvent` objects](#section1.1)
    -   [Example](#section1.2)
-   [Administration guide](#section2)
    -   [Configuration](#section2.1)

## <a name="section1"></a>Functionality

### <a name="section1.1"></a>Mapping NGSI events to `NGSIEvent` objects

This section explains how a notified NGSI event (a http message containing headers and payload) is used to create a
`NGSIEvent` object, suitable for being consumed by any of the Draco processors, thanks to `ListenHTTP`.

It is necessary to remark again this handler is designed for being used by `ListenHTTP`, the native processor of Apache
NiFi. An http message containing a NGSI-like notification will be received by `ListenHTTP` and passed to
`NGSIRestHandler` in order to create one or more `NGSIEvent` objects (one per notified context element) to be put in a
sink's channel (mainly, these channels are objects in memory, but could be files).

On the one hand, the http message containing the NGSI-like notification will be composed of a set of http headers, and a
payload. On the other hand, the `NGSIEvent` objects are composed of a set of headers as well and an object of type
`ContextElement` containing the already parsed version of the context elements within the notification; this parsed
version of the notified body is ready for being consumed by other components in the agent architecture, such as
interceptors or sinks, thus parsing is just made once.

As can be seen, there is a quasi-direct translation among http messages and `NGSIEvent` objects:

| http message                 | `NGSIEvent` object                                                                                                        |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `Fiware-Service` header      | `fiware-service` header                                                                                                   |
|  `Fiware-ServicePath` header | `fiware-servicepath` header                                                                                               |
|  `Fiware-Correlator` header  | `fiware-correlator` header. If this header is not sent, the `fiware-correlator` is equals to the `transaction-id` header. |
|                              | `transaction-id` header (internally added)                                                                                |
| any other header             | discarded                                                                                                                 |
|  payload                     | `ContextElement` object containing the parsed version of the payload                                                      |

All the FIWARE headers are added to the `NGSIEvent` object if notified. If not, default values are used (it is the case
of `fiware-service` and `fiware-servicepath`, which take the configured value of `default_service` and
`default_service_path` respectively, see below the configuration section) or auto-generated (it is the case of
`fiware-correlator`, whose value is the same than `transaction-id`).

As already introduced, in addition to the `fiware-correlator`, a `transaction-id` is created for internally identify a
complete Draco transaction, i.e. starting at the source when the context data is notified, and finishing in the sink,
where such data is finally persisted. If `Fiware-Correlator` header is not notified, then `fiware-correlator` and
`transactionid` get the same auto-generated value.

Finally, it must be said the `NGSIEVent` contains another field, of type `ContextElement` as well, in order the
`NGSINameMappingsInterceptor` add a mapped version of the original context element added by this handler.

### <a name="section1.2"></a>Example

Let's assume the following not-intercepted event regarding a received notification (the code below is an <i>object
representation</i>, not any real data format):

```
notification={
   headers={
	   fiware-service=hotel1,
	   fiware-servicepath=/other,/suites,
	   correlation-id=1234567890-0000-1234567890
   },
   body={
      {
	      entityId=suite.12,
	      entityType=room,
	      attributes=[
	         ...
	      ]
	   },
	   {
	      entityId=other.9,
	      entityType=room,
	      attributes=[
	         ...
	      ]
	   }
	}
}
```

As can be seen, two entities (`suite.12` and `other.9`) of the same type (`room`) within the same FIWARE service
(`hotel`) but different service paths (`/suites` and `/other`) are notified. `NGSIRestHandler` will create two
`NGSIEvent`'s:

```
ngsi-event-1={
   headers={
	   fiware-service=hotel,
	   fiware-servicepath=/suites,
	   transaction-id=1234567890-0000-1234567890,
	   correlation-id=1234567890-0000-1234567890,
	   timestamp=1234567890,
	   mapped-fiware-service=hotel
	   mapped-fiware-service-path=/suites
	},
   original-context-element={
	   entityId=suite.12,
	   entityType=room,
	   attributes=[
	      ...
	   ]
	}
}

ngsi-event-2={
   headers={
	   fiware-service=hotel,
	   fiware-servicepath=/other,
	   transaction-id=1234567890-0000-1234567890,
	   correlation-id=1234567890-0000-1234567890,
	   timestamp=1234567890,
	   mapped-fiware-service=hotel
	   mapped-fiware-service-path=/other
   },
   original-context-element={
	   entityId=other.9,
	   entityType=room,
	   attributes=[
	      ...
	   ]
	}
}
```

## <a name="section2"></a>Administration guide

### <a name="section2.1"></a>Configuration

`ListenHTTP` is configured through the following parameters:

| Name                                              | Default Value   | Allowable Values                                                                                       | Description                                                                                                                                                                                                                 |
| ------------------------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Base Path**                                     | contentListener |                                                                                                        | Base path for incoming connectionsSupports, this has to match with the notify attribute of the subscription made in ORION Expression Language: true (will be evaluated using variable registry only)                        |
| **Listening Port**                                | no              |                                                                                                        | The Port to listen on for incoming connectionsSupports, also need to be including in the subscription, Expression Language: true (will be evaluated using variable registry only)                                           |
| Max Data to Receive per Second                    |                 |                                                                                                        | The maximum amount of data to receive per second; this allows the bandwidth to be throttled to a specified data rate; if not specified, the data rate is not throttled                                                      |
| SSL Context Service                               |                 | Controller Service API: RestrictedSSLContextServiceImplementation: StandardRestrictedSSLContextService | The Controller Service to use in order to obtain an SSL Context                                                                                                                                                             |
| Authorized DN Pattern                             |                 | .\*                                                                                                    |                                                                                                                                                                                                                             | A Regular Expression to apply against the Distinguished Name of incoming connections. If the Pattern does not match the DN, the connection will be refused. |
| Max Unconfirmed Flowfile Time                     | 60 secs         |                                                                                                        | The maximum amount of time to wait for a FlowFile to be confirmed before it is removed from the cache                                                                                                                       |
| **HTTP Headers to receive as Attributes (Regex)** | no              |                                                                                                        | Specifies the Regular Expression that determines the names of HTTP Headers that should be passed along as FlowFile attributes. You have to include at least Fiware-service, Fiware-Service-Path and Optionally X-Auth-Token |
| Return Code                                       | 200             |                                                                                                        | The HTTP return code returned after every HTTP call                                                                                                                                                                         |

A configuration example could be:

![listen-processor](../images/processor-http.png)
