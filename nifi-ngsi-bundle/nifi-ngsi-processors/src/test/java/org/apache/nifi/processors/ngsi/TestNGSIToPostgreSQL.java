package org.apache.nifi.processors.ngsi;

import org.apache.nifi.processors.ngsi.ngsi.backends.PostgreSQLBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIConstants.POSTGRESQL_COLUMN_TYPES;
import org.json.JSONArray;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class TestNGSIToPostgreSQL {
    private PostgreSQLBackend backend = new PostgreSQLBackend();

    private NGSIUtils ngsiUtils = new NGSIUtils();

    private InputStream inputStream = getClass().getClassLoader().getResourceAsStream("temporalEntity.json");

    @Test
    public void testValuesForInsertColumnForNgsiLd() throws IOException {
        String data = readFromInputStream(inputStream);
        ArrayList<Entity> entities = ngsiUtils.parseNgsiLdEntities(new JSONArray(data));

        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields = backend.listOfFields(entities.get(3), "");

        long creationTime = 1562561734983l;
        TimeZone.setDefault(TimeZone.getTimeZone("CEST"));
        ZonedDateTime creationDate = Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC);

        List<String> timeStamps = entities.get(3).getEntityAttrsLD().stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt)).keySet().stream().collect(Collectors.toList());

        String expectedValuesForInsert = "('0.34944123-0.32415766-0.3072652-0.30855495-0.30593967-0.3028966-0.30651844-0.30827406-0.31077954-0.31049716-0.30885014-0.30811214-0.3043771-0.3003832-0.2957132-0.2931525-0.29560313-0.2964879-0.29803914-0.2990379-0.30066556-0.29994768-0.29871973-0.29959038-0.2995688-0.29707655-0.2986042-0.2964256-0.29641816-0.29547918-0.291907-0.29169783-0.2903415-0.28780755-0.28604454-0.2864968-0.28618598-0.28226927-0.28288576-0.28220624-0.27975032-0.2795406-0.278664-0.27759662-0.27686507-0.27530807-0.2749262-0.27412018-0.2732852-0.2715867-0.27154332-0.27261332-0.27118307-0.27099013-0.27138162-0.27050668-0.2699476-0.2716659-0.26954666-0.2695933-0.26745608-0.26783624-0.26756752-0.26854938-0.2686887-0.26828912-0.26727334-0.26540932-0.2656524-0.2655901-0.26567256-0.26666725-0.26808578-0.26682115-0.26708144-0.26603967-0.26675877-0.26803017-0.27002114-0.26863557-0.2672063-0.2682099-0.26853663-0.26881462-0.26931205-0.27029905-0.27124438-0.2721271-0.27170712-0.27188423-0.27396426-0.27401495-0.27322963-0.27323526-0.27423725-0.27355447-0.27317145-0.27363068-0.2724783-0.2739294-0.27332857-0.27178755-0.27148366-0.27160507-0.27099782-0.2717871-0.27076796-0.2723923-0.27258486-0.2734231-0.2722952-0.27318266-0.27287984-0.27171746-0.273019-0.2721147-0.27194858-0.27121463-0.27036-0.26990783-0.26930192-0.26793072-0.26803744-0.26680467-0.26759195-0.26783347-0.26707804-0.26733038-0.26691008-0.2671964-0.2681563-0.2681742-0.26864722-0.26975495-0.2696539-0.27219784-0.27067336-0.27206865-0.27377665-0.27501073-0.275479-0.27633333-0.27576253-0.27492747-0.27466607-0.27668217-0.2787584-0.2803111-0.2826895-0.2851108-0.28731674-0.29144207-0.29401872-0.29914567-0.30502665-0.31111237-0.31869608-0.32225254-0.3278929-0.33499655-0.34023458-0.34320205-0.34701312-0.35116884-0.35364178-0.35598436-0.3580442-0.36053157-0.36390346-0.36393243-0.36471847-0.36623812-0.36946937-0.37243837-0.37273547-0.37479842-0.37529087-0.37477198-0.3754256-0.3748324-0.37489796-0.37413442-0.37510356-0.37357545-0.371788-0.3716578-0.36927506-0.36673775-0.36606938-0.36443707-0.36090052-0.3608684-0.36165372-0.36146113-0.36150908-0.36063817-0.3614172-0.36146975-0.36100012-0.3598923-0.36086115-0.36045706-0.35982618-0.35832256-0.35752884-0.358617-0.3565569-0.35802928-0.3568049-0.35795924-0.35754114-0.3568314-0.3569109-0.35581905-0.35641986-0.3545677-0.35477188-0.35241178-0.35025528-0.3484056-0.34673157-0.34454417-0.3431547-0.33934793-0.33773395-0.33757868-0.33252433-0.3313897-0.33280596-0.32944524-0.3221002-0.3226984-0.3204581-0.3224731-0.32046703-0.32059407-0.32100388-0.3216591-0.31857193-0.32001126-0.3188547-0.31097677-0.2950546-0.27814576-0.26328075-0.24463399-0.22101705-0.20268844-0.18668799-0.17968106-0.17162262-0.17417294-0.17698781-0.19054312-0.19953361-0.21006162-','2021-02-16T12:36:35.000000Z','urn:ngsi-ld:AgriCropRecord:gael:ble:35cf0844-bdb7-44b8-ac87-eea077343a10','AgriCropRecord','14','2021-02-16T12:36:35.000000Z','67','2021-02-16T12:36:35.000000Z',5.82,'2021-02-16T12:36:35.000000Z','1026','2021-02-16T12:36:35.000000Z','237','2021-02-16T12:36:35.000000Z','12.4','2021-02-16T12:36:35.000000Z','2019-07-08T04:55:34.983Z')";

        List<String> valuesForInsert = backend.getValuesForInsert(entities.get(3), listOfFields, creationTime, "");
        assertEquals(2, valuesForInsert.size());
        assertEquals(expectedValuesForInsert, valuesForInsert.get(0));
        for (int i = 0; i < timeStamps.size(); i++) {
            assertTrue(valuesForInsert.get(i).contains(timeStamps.get(i)));
            assertTrue(valuesForInsert.get(i).contains(DateTimeFormatter.ISO_INSTANT.format(creationDate)));
        }
    }

    @Test
    public void testInsertQueryForNgsiLd() throws Exception {
        String data = readFromInputStream(inputStream);
        ArrayList<Entity> entities = ngsiUtils.parseNgsiLdEntities(new JSONArray(data));

        String schemaName = backend.buildSchemaName("test",false,false);
        String tableName = backend.buildTableName(entities.get(3),"db-by-entity-type",true, false);

        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields = backend.listOfFields(entities.get(3), "");

        long creationTime = 1562561734983l;

        String instertQueryValue = backend.insertQuery(
                entities.get(3),
                creationTime,
                schemaName,
                tableName,
                listOfFields,
                ""
        );

        assertTrue(instertQueryValue.split("\n").length ==2);
    }

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
}
