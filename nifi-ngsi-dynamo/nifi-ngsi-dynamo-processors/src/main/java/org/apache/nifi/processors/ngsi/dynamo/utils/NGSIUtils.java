package org.apache.nifi.processors.ngsi.dynamo.utils;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;


public class NGSIUtils {

    public NGSIEvent getEventFromFlowFile(FlowFile flowFile, final ProcessSession session, String version){

        final byte[] buffer = new byte[(int) flowFile.getSize()];

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });
        // Create the PreparedStatement to use for this FlowFile.
        Map flowFileAttributes = flowFile.getAttributes();
        Map <String,String> newFlowFileAttributes = new CaseInsensitiveMap(flowFileAttributes);
        final String flowFileContent = new String(buffer, StandardCharsets.UTF_8);
        String fiwareService = (newFlowFileAttributes.get("fiware-service") == null) ? "nd":newFlowFileAttributes.get("fiware-service");
        String fiwareServicePath = (newFlowFileAttributes.get("fiware-servicepath")==null) ? "/nd":newFlowFileAttributes.get("fiware-servicepath");
        long creationTime=flowFile.getEntryDate();
        JSONObject content = new JSONObject(flowFileContent);
        JSONArray data;
        String entityType;
        String entityId;
        ArrayList<Entity> entities = new ArrayList<>();
        NGSIEvent event;

        if("v2".compareToIgnoreCase(version)==0){
            data = (JSONArray) content.get("data");
            for (int i = 0; i < data.length(); i++) {
                JSONObject lData = data.getJSONObject(i);
                entityId = lData.getString("id");
                entityType = lData.getString("type");
                ArrayList<Attributes> attrs  = new ArrayList<>();
                Iterator<String> keys = lData.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if (!"id".equals(key) && !"type".equals(key)){
                        JSONObject value = lData.getJSONObject(key);
                        JSONObject mtdo = (JSONObject) value.get("metadata");
                        Iterator<String> keysOneLevel=mtdo.keys();
                        String metadataString = value.get("metadata").toString();
                        ArrayList<Metadata>  mtd = new ArrayList<>();
                        while (keysOneLevel.hasNext()) {
                            String keyOne = keysOneLevel.next();
                            JSONObject value2 = mtdo.getJSONObject(keyOne);
                            mtd.add(new Metadata(keyOne,value2.getString("type"),value2.get("value").toString()));
                        }
                        if(mtdo.length()<=0){
                            attrs.add(new Attributes(key,value.getString("type"),value.get("value").toString(),null,""));
                        }else{
                            attrs.add(new Attributes(key,value.getString("type"),value.get("value").toString(),mtd,metadataString));
                        }
                    }
                }
                entities.add(new Entity(entityId,entityType,attrs));
            }
        }else if ("ld".compareToIgnoreCase(version)==0){
            System.out.println("Work in progress");
        }
        event = new NGSIEvent(creationTime,fiwareService,fiwareServicePath,entities);
        return event;
    }
}