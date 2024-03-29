curl -X POST 'http://localhost:5050/ld/notify' -H 'Content-Type: application/ld+json' -H 'Fiware-service: moncloa' -d @- <<EOF 
{
  "id": "urn:ngsi-ld:Notification:352334523452",
  "type": "Notification",
  "subscriptionId": "urn:ngsi-ld:Subscription:5e6257789f300bcd70b7b6e",
  "data": [
    {
        "id": "urn:ngsi:ld:OffStreetParking:Downtown0005",
        "type": "OffStreetParking",
        "@context": [
        "http://example.org/ngsi-ld/parking.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"],
        "name": {
        "type": "Property",
           "value": "Downtown One"
        },
        "availableSpotNumber": {
            "type": "Property",
            "value": 122,
            "observedAt": "2017-07-29T12:05:02Z",
            "reliability": {
                "type": "Property",
                "value": 0.7
            },
            "providedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Camera:C1"
            }
        },
        "totalSpotNumber": {
            "type": "Property",
            "value": 200,
            "observedAt": "2017-07-29T12:05:02Z",
            "unitCode": "5K"
        },
        "newAttr": {
            "type": "Property",
            "value": 000,
            "observedAt": "2007-07-29T12:05:02Z",
            "unitCode": "50K"
        },
            "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [-8.5, 41.2]
            }
        }
    }
  ]
}
EOF
