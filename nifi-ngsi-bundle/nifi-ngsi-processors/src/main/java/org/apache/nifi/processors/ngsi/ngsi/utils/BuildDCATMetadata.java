package org.apache.nifi.processors.ngsi.ngsi.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class BuildDCATMetadata {
    public DCATMetadata getMetadataFromFlowFile(FlowFile flowFile, final ProcessSession session) {

        final byte[] buffer = new byte[(int) flowFile.getSize()];

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });
        // Create the PreparedStatement to use for this FlowFile.
        Map flowFileAttributes = flowFile.getAttributes();
        Map<String, String> newFlowFileAttributes = new CaseInsensitiveMap(flowFileAttributes);
        String [] keywords = newFlowFileAttributes.get("keywords").replace("[", "").replace("]","").replaceAll("\"","").split(",");

        final DCATMetadata dcatMetadata = new DCATMetadata(
                newFlowFileAttributes.get("organizationName"),
                newFlowFileAttributes.get("organizationType"),
                newFlowFileAttributes.get("packageDescription"),
                newFlowFileAttributes.get("packageName"),
                newFlowFileAttributes.get("contactPoint"),
                newFlowFileAttributes.get("contactName"),
                newFlowFileAttributes.get("contactEmail"),
                keywords,
                newFlowFileAttributes.get("publisherURL"),
                newFlowFileAttributes.get("spatialUri"),
                newFlowFileAttributes.get("spatialCoverage"),
                newFlowFileAttributes.get("temporalStart"),
                newFlowFileAttributes.get("temporalEnd"),
                newFlowFileAttributes.get("themes"),
                newFlowFileAttributes.get("version"),
                newFlowFileAttributes.get("landingPage"),
                newFlowFileAttributes.get("visibility"),
                newFlowFileAttributes.get("accessURL"),
                newFlowFileAttributes.get("availability"),
                newFlowFileAttributes.get("resourceDescription"),
                newFlowFileAttributes.get("format"),
                newFlowFileAttributes.get("mimeType"),
                newFlowFileAttributes.get("license"),
                newFlowFileAttributes.get("licenseType"),
                newFlowFileAttributes.get("downloadURL"),
                newFlowFileAttributes.get("byteSize"),
                newFlowFileAttributes.get("resourceName")
        );
        return dcatMetadata;
    }
}
