package org.apache.nifi.processors.ngsi.ngsi.utils;

import org.apache.nifi.processors.ngsi.ngsi.backends.PostgreSQLBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIConstants.POSTGRESQL_COLUMN_TYPES;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class TestJsonNgsi {

    public static void main(String[] args) throws Exception {
        /*String json= "{\n" +
                "   \"data\": [\n" +
                "      {\n" +
                "         \"id\": \"Room10\",\n" +
                "         \"temperature10\": {\n" +
                "            \"metadata\": {\n" +
                "               \"accuracy10\": {\n" +
                "                  \"value\": 0.88,\n" +
                "                  \"type\": \"Float\"\n" +
                "               }\n" +
                "            },\n" +
                "            \"type\": \"Float\",\n" +
                "            \"value\": 244.5\n" +
                "         },\n" +
                "         \"temperature20\": {\n" +
                "            \"metadata\": {\n" +
                "               \"accuracy20\": {\n" +
                "                  \"value\": 0.882,\n" +
                "                  \"type\": \"Float\"\n" +
                "               },\n" +
                "               \"accuracy30\": {\n" +
                "                  \"value\": 0.885,\n" +
                "                  \"type\": \"Float\"\n" +
                "               }\n" +
                "            },\n" +
                "            \"type\": \"Float\",\n" +
                "            \"value\": 233.5\n" +
                "         },\n" +
                "         \"temperature30\": {\n" +
                "            \"metadata\": {},\n" +
                "            \"type\": \"Float\",\n" +
                "            \"value\": 222.5\n" +
                "         },\n" +
                "         \"temperature40\": {\n" +
                "            \"metadata\": {},\n" +
                "            \"type\": \"Float\",\n" +
                "            \"value\": 211.5\n" +
                "         },\n" +
                "         \"type\": \"Room\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"id\": \"Room2\",\n" +
                "         \"temperature2\": {\n" +
                "            \"metadata\": {\n" +
                "               \"accuracy2\": {\n" +
                "                  \"value\": 0.8,\n" +
                "                  \"type\": \"Float\"\n" +
                "               }\n" +
                "            },\n" +
                "            \"type\": \"Float\",\n" +
                "            \"value\": 24.5\n" +
                "         },\n" +
                "         \"temperature3\": {\n" +
                "            \"metadata\": {\n" +
                "               \"accuracy4\": {\n" +
                "                  \"value\": 0.82,\n" +
                "                  \"type\": \"Float\"\n" +
                "               },\n" +
                "               \"accuracy5\": {\n" +
                "                  \"value\": 0.85,\n" +
                "                  \"type\": \"Float\"\n" +
                "               }\n" +
                "            },\n" +
                "            \"type\": \"Float\",\n" +
                "            \"value\": 23.5\n" +
                "         },\n" +
                "         \"temperature4\": {\n" +
                "            \"metadata\": {},\n" +
                "            \"type\": \"Float\",\n" +
                "            \"value\": 22.5\n" +
                "         },\n" +
                "         \"temperature5\": {\n" +
                "            \"metadata\": {},\n" +
                "            \"type\": \"Float\",\n" +
                "            \"value\": 21.5\n" +
                "         },\n" +
                "         \"type\": \"Room---\"\n" +
                "      }\n" +
                "   ],\n" +
                "   \"subscriptionId\": \"57458eb60962ef754e7c0998\"\n" +
                "}";*/


        String json="{\n" +
                 "    \"id\": \"urn:ngsi-ld:Vehicle:V123\",\n" +
                 "    \"type\": \"Vehicle\",\n" +
                 "    \"speed\": {\n" +
                 "      \"type\": \"Property\",\n" +
                 "      \"value\": 23,\n" +
                 "      \"accuracy\": {\n" +
                 "        \"type\": \"Property\",\n" +
                 "        \"value\": 0.7\n" +
                 "      },\n" +
                 "      \"providedBy\": {\n" +
                 "        \"type\": \"Relationship\",\n" +
                 "        \"object\": \"urn:ngsi-ld:Person:Bob\"\n" +
                 "      }\n" +
                 "    },\n" +
                 "    \"closeTo\": {\n" +
                 "      \"type\": \"Relationship\",\n" +
                 "      \"object\": \"urn:ngsi-ld:Building:B1234\"\n" +
                 "    },\n" +
                 "    \"location\": {\n" +
                 "        \"type\": \"GeoProperty\",\n" +
                 "        \"value\": {\n" +
                 "          \"type\":\"Point\",\n" +
                 "          \"coordinates\": [-8,44]\n" +
                 "        }\n" +
                 "    },\n" +
                 "    \"@context\": [\n" +
                 "        \"https://example.org/ld/vehicle.jsonld\",\n" +
                 "        \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\"\n" +
                 "    ]\n" +
                 "  },\n" +
                 "  {\n" +
                 "    \"id\": \"urn:ngsi-ld:OffStreetParking:Downtown1\",\n" +
                 "    \"type\": \"OffStreetParking\",\n" +
                 "    \"name\": {\n" +
                 "      \"type\": \"Property\",\n" +
                 "        \"value\": \"Downtown One\"\n" +
                 "    },\n" +
                 "    \"availableSpotNumber\": {\n" +
                 "      \"type\": \"Property\",\n" +
                 "      \"value\": 121,\n" +
                 "      \"observedAt\": \"2017-07-29T12:05:02Z\",\n" +
                 "      \"reliability\": {\n" +
                 "            \"type\": \"Property\",\n" +
                 "            \"value\": 0.7\n" +
                 "      },\n" +
                 "      \"providedBy\": {\n" +
                 "            \"type\": \"Relationship\",\n" +
                 "            \"object\": \"urn:ngsi-ld:Camera:C1\"\n" +
                 "      }\n" +
                 "    },\n" +
                 "    \"totalSpotNumber\": {\n" +
                 "        \"type\": \"Property\",\n" +
                 "        \"value\": 200,\n" +
                "        \"observedAt\": \"2017-07-29T12:05:02Z\",\n" +
                "        \"unitCode\": 5K\n" +
                 "    },\n" +
                 "    \"location\": {\n" +
                 "      \"type\": \"GeoProperty\",\n" +
                 "      \"value\": {\n" +
                 "        \"type\": \"Point\",\n" +
                 "        \"coordinates\": [-8.5, 41.2]\n" +
                 "      }\n" +
                 "    },\n" +
                 "    \"@context\": [\n" +
                 "        \"http://example.org/ngsi-ld/parking.jsonld\",\n" +
                 "        \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\"\n" +
                 "    ]\n" +
                 "  }";
        final String notification = ""
                + "{"
                +   "\"subscriptionId\" : \"51c0ac9ed714fb3b37d7d5a8\","
                +   "\"data\" : ["
                +          json
                +   "]"
                + "}";
        System.out.println(notification);

        JSONObject content = new JSONObject(notification);
        JSONArray data;
        String context = "";
        String entityId = "";
        String entityType = "";
        System.out.println("Work in progress");
        ArrayList<Entity> entitiesLD = new ArrayList<>();
        NGSIEvent event;
        boolean hasSubAttrs= false;
        data = (JSONArray) content.get("data");
        for (int i = 0; i < data.length(); i++) {
            JSONObject lData = data.getJSONObject(i);
            entityId = lData.getString("id");
            entityType = lData.getString("type");
            ArrayList<AttributesLD> attributes  = new ArrayList<>();
            Iterator<String> keys = lData.keys();
            String attrType="";
            String attrValue="";
            String subAttrName="";
            String subAttrType="";
            String subAttrValue="";
            ArrayList<AttributesLD> subAttributes=new ArrayList<>();

            while (keys.hasNext()) {
                String key = keys.next();
                if (!"id".equals(key) && !"type".equals(key) && !"@context".equals(key)){
                    JSONObject value = lData.getJSONObject(key);
                    attrType = value.getString("type");
                    if ("Relationship".contentEquals(attrType)){
                        attrValue = value.get("object").toString();
                    }else if ("Property".contentEquals(attrType)){
                        attrValue = value.get("value").toString();
                        System.out.println(value);
                            Iterator<String> keysOneLevel = value.keys();
                        System.out.println("************");
                            while (keysOneLevel.hasNext()) {
                                String keyOne = keysOneLevel.next();
                                if ("type".equals(keyOne)){
                                    // Do Nothing
                                } else if ("observedAt".equals(keyOne) || "unitCode".equals(keyOne)){
                                    // TBD Do Something for unitCode and observedAt
                                    String value2 = value.getString(keyOne);
                                    subAttrName = keyOne;
                                    subAttrValue = value2;
                                    hasSubAttrs = true;
                                    subAttributes.add(new AttributesLD(subAttrName,subAttrValue,"", "", "","", subAttrValue,false,null));
                                }
                                else if (!"value".equals(keyOne)){
                                    JSONObject value2 = value.getJSONObject(keyOne);
                                    subAttrName=keyOne;
                                    subAttrType=value2.get("type").toString();
                                    System.out.println(value2);
                                    if ("Relationship".contentEquals(subAttrType)){
                                        subAttrValue = value2.get("object").toString();
                                    }else if ("Property".contentEquals(subAttrType)){
                                        subAttrValue = value2.get("value").toString();
                                    }else if ("GeoProperty".contentEquals(subAttrType)){
                                        subAttrValue = value2.get("value").toString();
                                    }
                                    System.out.println(subAttrName);
                                    System.out.println(subAttrType);
                                    System.out.println(subAttrValue);
                                    hasSubAttrs= true;
                                    subAttributes.add(new AttributesLD(subAttrName,subAttrType,"", "", "","", subAttrValue,false,null));
                                }
                            }
                    }else if ("GeoProperty".contentEquals(attrType)){
                        attrValue = value.get("value").toString();
                    }
                    attributes.add(new AttributesLD(key,attrType,"", "", "", "", attrValue, hasSubAttrs,subAttributes));
                    subAttributes=new ArrayList<>();
                    hasSubAttrs= false;
                }
            }
            entitiesLD.add(new Entity(entityId,entityType,attributes,true));
        }

        PostgreSQLBackend p = new PostgreSQLBackend();

        event = new NGSIEvent(000,"2",entitiesLD);
        for (Entity x: event.getEntitiesLD()){
            p.listOfFields("column",x,"ld",false, "");
        }

        for (Entity x: event.getEntitiesLD()){
            Map<String, POSTGRESQL_COLUMN_TYPES> list=p.listOfFields("column",x,"ld",false, "");
            System.out.println(p.getFieldsForCreate(list));
            System.out.println(p.getFieldsForInsert(list.keySet()));
            System.out.println(p.getFieldsForInsert(list.keySet()));
            System.out.println(p.buildSchemaName("test",false,false,false));
            System.out.println(p.buildTableName("",x,"db-by-entity-type",true,true,"ld",false, null));
            Map<String, POSTGRESQL_COLUMN_TYPES> listC = new HashMap<>();
            listC.put("a", POSTGRESQL_COLUMN_TYPES.TEXT);
            listC.put("b", POSTGRESQL_COLUMN_TYPES.TEXT);
            listC.put("c", POSTGRESQL_COLUMN_TYPES.TEXT);

            System.out.println(p.addColumns("test","test",listC));

            System.out.println(p.getValuesForInsert("column",x, Collections.emptyMap(), 000,"","ld", false,""));
            for (AttributesLD y :x.getEntityAttrsLD()){
                System.out.println(y.getAttrName()+"-------");
                System.out.println(y.isHasSubAttrs());
                if (y.isHasSubAttrs()){
                    y.getSubAttrs().forEach(z->System.out.println(z.getAttrName()));
                }
            }
        }
    }
}
