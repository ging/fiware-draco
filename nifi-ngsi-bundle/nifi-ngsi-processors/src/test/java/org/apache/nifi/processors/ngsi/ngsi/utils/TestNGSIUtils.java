package org.apache.nifi.processors.ngsi.ngsi.utils;

import org.json.JSONArray;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class TestNGSIUtils {

    private String readFromInputStream(InputStream inputStream)
            throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }

    private InputStream inputStream = getClass().getClassLoader().getResourceAsStream("temporalEntity.json");

    private NGSIUtils ngsiUtils = new NGSIUtils();

    @Test
    public void testTemporalEntities() throws IOException {
        String data = readFromInputStream(inputStream);
        ArrayList<Entity> entities = ngsiUtils.parseNgsiLdEntities(new JSONArray(data));
        assertEquals(3, entities.size());

        ArrayList<AttributesLD> attributes = entities.get(0).entityAttrsLD;
        Map<String, List<AttributesLD>> attributesByObservedAt = attributes.stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt));
        assertEquals(4, attributesByObservedAt.size());
    }
}