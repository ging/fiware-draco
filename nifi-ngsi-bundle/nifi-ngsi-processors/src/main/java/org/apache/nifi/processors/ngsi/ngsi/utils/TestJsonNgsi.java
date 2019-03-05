package org.apache.nifi.processors.ngsi.ngsi.utils;

import org.json.JSONArray;
import org.json.JSONObject;

public class TestJsonNgsi {

    public static void main(String[] args){
        String json= "{\n" +
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
                "}";
    }

}
