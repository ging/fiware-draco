package org.apache.nifi.processors.ngsi.ngsi.backends.ckan;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.Header;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.nifi.processors.ngsi.ngsi.backends.ckan.model.DataStore;
import org.apache.nifi.processors.ngsi.ngsi.backends.http.HttpBackend;
import org.apache.nifi.processors.ngsi.ngsi.backends.http.JsonResponse;
import org.apache.nifi.processors.ngsi.ngsi.utils.*;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

public class CKANBackend extends HttpBackend {

    private static final int RECORDSPERPAGE = 100;
    private final String orionUrl;
    private final String apiKey;
    private final String viewer;
    private CKANCache cache;

    /**
     * Constructor.
     * @param apiKey
     * @param ckanHost
     * @param ckanPort
     * @param orionUrl
     * @param ssl
     * @param maxConns
     * @param maxConnsPerRoute
     * @param ckanViewer
     */
    public CKANBackend(String apiKey, String[] ckanHost, String ckanPort, String orionUrl,
                       boolean ssl, int maxConns, int maxConnsPerRoute, String ckanViewer) {
        super(ckanHost, ckanPort, ssl, false, null, null, null, null);

        // this class attributes
        this.apiKey = apiKey;
        this.orionUrl = orionUrl;
        this.viewer = ckanViewer;

        // create the cache
        cache = new CKANCache(ckanHost, ckanPort, ssl, apiKey, maxConns, maxConnsPerRoute);
    } // CKANBackendImpl

    public void persist(String orgName, String pkgName, String resName, String records, boolean createEnabled, DCATMetadata dcatMetadata, boolean createDataStore)
            throws Exception {
        System.out.println("Going to lookup for the resource id, the cache may be updated during the process (orgName="
                + orgName + ", pkgName=" + pkgName + ", resName=" + resName + ")");
        String resId = "";
        if (!createEnabled) {
            resId= resourceLookupOrCreateDynamicFields(orgName, pkgName, resName,records, dcatMetadata,createDataStore);
        } else {
            resId = resourceLookupOrCreate(orgName, pkgName, resName, createEnabled, dcatMetadata,createDataStore);
        }
        if (resId == null) {
            throw new Exception("Cannot persist the data (orgName=" + orgName + ", pkgName=" + pkgName
                    + ", resName=" + resName + ")");
        } else {
            if (createDataStore){
                System.out.println("Going to persist the data (orgName=" + orgName + ", pkgName=" + pkgName
                    + ", resName/resId=" + resName + "/" + resId + ")");

                    insert(resId, records);
                }
            else{
                System.out.println("DataStore was not created in the resource (orgName=" + orgName + ", pkgName=" + pkgName
                        + ", resName/resId=" + resName + "/" + resId + ")");
                }
        } // if else
    } // persist

    /**
     * Lookup or create resources used by cygnus-ngsi-ld and create the datastore with the fields available in the records parameter.
     * @param orgName The organization in which datastore the record is going to be inserted
     * @param pkgName The package name to be created or lookup to
     * @param resName The resource name to be created or lookup to
     * @param records Te records to be inserted and used to create the datastore fields
     * @throws Exception
     */
    private String resourceLookupOrCreateDynamicFields(String orgName, String pkgName, String resName, String records,DCATMetadata dcatMetadata, boolean createDataStore)
            throws Exception {
        if (!cache.isCachedOrg(orgName)) {
            System.out.println("The organization was not cached nor existed in CKAN (orgName=" + orgName + ")");
            
            String orgId = createOrganization(orgName,dcatMetadata);
            cache.addOrg(orgName);
            cache.setOrgId(orgName, orgId);
            String pkgId = createPackage(pkgName, orgId,dcatMetadata);
            cache.addPkg(orgName, pkgName);
            cache.setPkgId(orgName, pkgName, pkgId);
            String resId = createResource(resName, pkgId, dcatMetadata);
            cache.addRes(orgName, pkgName, resName);
            cache.setResId(orgName, pkgName, resName, resId);
            if(createDataStore){
                createDataStoreWithFields(resId,records);
                createView(resId);
            }
            return resId;
            // if else
        } // if

        System.out.println("The organization was cached (orgName=" + orgName + ")");

        if (!cache.isCachedPkg(orgName, pkgName)) {
            System.out.println("The package was not cached nor existed in CKAN (orgName=" + orgName + ", pkgName="
                    + pkgName + ")");

            String pkgId = createPackage(pkgName, cache.getOrgId(orgName), dcatMetadata);
            cache.addPkg(orgName, pkgName);
            cache.setPkgId(orgName, pkgName, pkgId);
            String resId = createResource(resName, pkgId, dcatMetadata);
            cache.addRes(orgName, pkgName, resName);
            cache.setResId(orgName, pkgName, resName, resId);
            if(createDataStore){
                createDataStoreWithFields(resId,records);
                createView(resId);
            }
            return resId;

        } // if

        System.out.println("The package was cached (orgName=" + orgName + ", pkgName=" + pkgName + ")");

        if (!cache.isCachedRes(orgName, pkgName, resName)) {
            System.out.println("The resource was not cached nor existed in CKAN (orgName=" + orgName + ", pkgName="
                    + pkgName + ", resName=" + resName + ")");

            String resId = this.createResource(resName, cache.getPkgId(orgName, pkgName), dcatMetadata);
            cache.addRes(orgName, pkgName, resName);
            cache.setResId(orgName, pkgName, resName, resId);
            if(createDataStore){
                createDataStoreWithFields(resId,records);
                createView(resId);
            }
            return resId;

        } // if

        System.out.println("The resource was cached (orgName=" + orgName + ", pkgName=" + pkgName + ", resName="
                + resName + ")");

        return cache.getResId(orgName, pkgName, resName);
    } // resourceLookupOrCreate

    private String resourceLookupOrCreate(String orgName, String pkgName, String resName, boolean createEnabled, DCATMetadata dcatMetadata, boolean createDataStore)
            throws Exception {
        if (!cache.isCachedOrg(orgName)) {
            System.out.println("The organization was not cached nor existed in CKAN (orgName=" + orgName + ")");

            if (createEnabled) {
                String orgId = createOrganization(orgName,dcatMetadata);
                cache.addOrg(orgName);
                cache.setOrgId(orgName, orgId);
                String pkgId = createPackage(pkgName, orgId, dcatMetadata);
                cache.addPkg(orgName, pkgName);
                cache.setPkgId(orgName, pkgName, pkgId);
                String resId = createResource(resName, pkgId, dcatMetadata);
                cache.addRes(orgName, pkgName, resName);
                cache.setResId(orgName, pkgName, resName, resId);
                if(createDataStore){
                    createDataStore(resId);
                    createView(resId);
                }
                return resId;
            } else {
                return null;
            } // if else
        } // if

        System.out.println("The organization was cached (orgName=" + orgName + ")");

        if (!cache.isCachedPkg(orgName, pkgName)) {
            System.out.println("The package was not cached nor existed in CKAN (orgName=" + orgName + ", pkgName="
                    + pkgName + ")");

            if (createEnabled) {
                String pkgId = createPackage(pkgName, cache.getOrgId(orgName), dcatMetadata);
                cache.addPkg(orgName, pkgName);
                cache.setPkgId(orgName, pkgName, pkgId);
                String resId = createResource(resName, pkgId, dcatMetadata);
                cache.addRes(orgName, pkgName, resName);
                cache.setResId(orgName, pkgName, resName, resId);
                if(createDataStore){
                    createDataStore(resId);
                    createView(resId);
                }
                return resId;
            } else {
                return null;
            } // if else
        } // if

        System.out.println("The package was cached (orgName=" + orgName + ", pkgName=" + pkgName + ")");

        if (!cache.isCachedRes(orgName, pkgName, resName)) {
            System.out.println("The resource was not cached nor existed in CKAN (orgName=" + orgName + ", pkgName="
                    + pkgName + ", resName=" + resName + ")");

            if (createEnabled) {
                String resId = this.createResource(resName, cache.getPkgId(orgName, pkgName), dcatMetadata);
                cache.addRes(orgName, pkgName, resName);
                cache.setResId(orgName, pkgName, resName, resId);
                if(createDataStore){
                    createDataStore(resId);
                    createView(resId);
                }
                return resId;
            } else {
                return null;
            } // if else
        } // if

        System.out.println("The resource was cached (orgName=" + orgName + ", pkgName=" + pkgName + ", resName="
                + resName + ")");

        return cache.getResId(orgName, pkgName, resName);
    } // resourceLookupOrCreate

    /**
     * Insert records in the datastore.
     * @param resId The resource in which datastore the record is going to be inserted
     * @param records Records to be inserted in Json format
     * @throws Exception
     */
    private void insert(String resId, String records) throws Exception {
        String jsonString = "{ \"resource_id\": \"" + resId
                + "\", \"records\": [ " + records + " ], "
                + "\"method\": \"insert\", "
                + "\"force\": \"true\" }";

        // create the CKAN request URL
        String urlPath = "/api/3/action/datastore_upsert";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

        // check the status
        if (res.getStatusCode() == 200) {
            System.out.println("Successful insert (resource/datastore id=" + resId + ")");
        } else {
            throw new Exception("Could not insert (resId=" + resId + ", statusCode="
                    + res.getStatusCode() + ")");
        } // if else
    } // insert

    /**
     * Creates an organization in CKAN.
     * @param orgName Organization to be created
     * @throws Exception
     * @return The organization id
     */
    private String createOrganization(String orgName, DCATMetadata dcatMetadata) throws Exception {
        // create the CKAN request JSON
        JsonArray extrasJsonArray = new JsonArray();
        JsonObject extrasJson = new JsonObject();
        JsonObject dataJson = new JsonObject();
        dataJson.addProperty("name",orgName);

        if (dcatMetadata!=null){
            dataJson.addProperty("title",orgName);
            dataJson.addProperty("name",orgName);

            //dataJson.add("extras",extrasJsonArray);
        }
        System.out.println(dataJson.toString());

        // create the CKAN request URL
        String urlPath = "/api/3/action/organization_create";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, dataJson.toString());

        // check the status
        if (res.getStatusCode() == 200) {
            String orgId = ((JSONObject) res.getJsonObject().get("result")).get("id").toString();
            System.out.println("Successful organization creation (orgName/OrgId=" + orgName + "/" + orgId + ")");
            return orgId;
        } else {
            throw new Exception("Could not create the orgnaization (orgName=" + orgName
                    + ", statusCode=" + res.getStatusCode() + ")");
        } // if else
    } // createOrganization

    /**
     * Creates a dataset/package within a given organization in CKAN.
     * @param pkgName Package to be created
     * @param orgId Organization the package belongs to
     * @return A package identifier if the package was created or an exception if something went wrong
     * @throws Exception
     */
    private String createPackage(String pkgName, String orgId, DCATMetadata dcatMetadata) throws Exception {
        // create the CKAN request JSON
        JsonArray extrasJsonArray = new JsonArray();
        JsonArray tagsJsonArray = new JsonArray();
        String[] keywords;
        JsonObject extrasJson = new JsonObject();
        JsonObject tags = new JsonObject();

        JsonObject dataJson = new JsonObject();
        dataJson.addProperty("name",pkgName);
        dataJson.addProperty("owner_org",orgId);

        if (dcatMetadata!=null){
            dataJson.addProperty("notes",dcatMetadata.getPackageDescription());
            dataJson.addProperty("title",pkgName);
            dataJson.addProperty("version",dcatMetadata.getVersion());
            dataJson.addProperty("url",dcatMetadata.getLandingPage());
            dataJson.addProperty("visibility",dcatMetadata.getVisibility());
            dataJson.addProperty("url",dcatMetadata.getLandingPage());


            extrasJson.addProperty("key","publisher_type");
            extrasJson.addProperty("value",dcatMetadata.getOrganizationType());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","contact_uri");
            extrasJson.addProperty("value",dcatMetadata.getContactPoint());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","contact_name");
            extrasJson.addProperty("value",dcatMetadata.getContactName());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","contact_email");
            extrasJson.addProperty("value",dcatMetadata.getContactEmail());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","spatial_uri");
            extrasJson.addProperty("value",dcatMetadata.getSpatialUri());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","spatial");
            extrasJson.addProperty("value",dcatMetadata.getSpatialCoverage());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","temporal_start");
            extrasJson.addProperty("value",dcatMetadata.getTemporalStart());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","temporal_end");
            extrasJson.addProperty("value",dcatMetadata.getTemporalEnd());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","theme");
            extrasJson.addProperty("value",dcatMetadata.getThemes());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","access_rights");
            extrasJson.addProperty("value",dcatMetadata.getDatasetRights());
            extrasJsonArray.add(extrasJson);


            //espacio para tags
            keywords=dcatMetadata.getKeywords();
            for (String tag: keywords){
                tags=new JsonObject();
                //tags.addProperty("vocabulary_id","null");
                tags.addProperty("name",tag);
                tagsJsonArray.add(tags);
            }
            //extrasJsonArray.add(extrasJson);}
            dataJson.add("extras",extrasJsonArray);
            dataJson.add("tags",tagsJsonArray);
        }
        System.out.println(dataJson.toString());
        // create the CKAN request URL
        String urlPath = "/api/3/action/package_create";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, dataJson.toString());

        // check the status
        if (res.getStatusCode() == 200) {
            String packageId = ((JSONObject) res.getJsonObject().get("result")).get("id").toString();
            System.out.println("Successful package creation (pkgName/pkgId=" + pkgName + "/" + packageId + ")");
            return packageId;
        /*
        This is not deleted if in the future we try to activate deleted elements again

        } else if (res.getStatusCode() == 409) {
            logger.debug("The package exists but its state is \"deleted\", activating it (pkgName="
                    + pkgName + ")");
            String packageId = activateElementState(httpClient, pkgName, "dataset");

            if (packageId != null) {
                logger.debug("Successful package activation (pkgId=" + packageId + ")");
                return packageId;
            } else {
                throw new CygnusRuntimeError("Could not activate the package (pkgId=" + pkgName + ")");
            } // if else
        */
        } else {
            throw new Exception("Could not create the package (orgId=" + orgId
                    + ", pkgName=" + pkgName + ", statusCode=" + res.getStatusCode() + ")");
        } // if else
    } // createPackage

    /**
     * Creates a resource within a given package in CKAN.
     * @param resName Resource to be created
     * @param pkgId Package the resource belongs to
     * @return A resource identifier if the resource was created or an exception if something went wrong
     * @throws Exception
     */
    private String createResource(String resName, String pkgId, DCATMetadata dcatMetadata) throws Exception {
        // create the CKAN request JSON

        JsonArray extrasJsonArray = new JsonArray();
        JsonObject extrasJson = new JsonObject();

        JsonObject dataJson = new JsonObject();
        dataJson.addProperty("package_id",pkgId);
        dataJson.addProperty("name",resName);
        dataJson.addProperty("access_url",dcatMetadata.getAccessURL());
        dataJson.addProperty("format",dcatMetadata.getFormat());
        dataJson.addProperty("availability",dcatMetadata.getAvailability());
        dataJson.addProperty("description",dcatMetadata.getResourceDescription());
        dataJson.addProperty("mimetype",dcatMetadata.getMimeType());
        dataJson.addProperty("license",dcatMetadata.getLicense());
        dataJson.addProperty("download_url",dcatMetadata.getDownloadURL());
        dataJson.addProperty("size",dcatMetadata.getByteSize());
        dataJson.addProperty("url",dcatMetadata.getDownloadURL());
        dataJson.addProperty("rights",dcatMetadata.getResourceRights());
        extrasJson.addProperty("key","license_type");
        extrasJson.addProperty("value",dcatMetadata.getLicenseType());
        //extrasJsonArray.add(extrasJson);
        dataJson.add("extras",extrasJsonArray);
        System.out.println(dataJson.toString());
        // create the CKAN request URL
        String urlPath = "/api/3/action/resource_create";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, dataJson.toString());

        // check the status
        if (res.getStatusCode() == 200) {
            String resourceId = ((JSONObject) res.getJsonObject().get("result")).get("id").toString();
            System.out.println("Successful resource creation (resName/resId=" + resName + "/" + resourceId
                    + ")");
            return resourceId;
        } else {
            throw new Exception("Could not create the resource (pkgId=" + pkgId
                    + ", resName=" + resName + ", statusCode=" + res.getStatusCode() + ")");
        } // if else
    } // createResource

    /**
     * Creates a datastore for a given resource in CKAN.
     * @param resId Identifies the resource whose datastore is going to be created.
     * @throws Exception
     */
    private void createDataStore(String resId) throws Exception {
        // create the CKAN request JSON
        // CKAN types reference: http://docs.ckan.org/en/ckan-2.2/datastore.html#valid-types
        String jsonString = "{ \"resource_id\": \"" + resId
                + "\", \"fields\": [ "
                + "{ \"id\": \"" + NGSIConstants.RECV_TIME_TS + "\", \"type\": \"int\"},"
                + "{ \"id\": \"" + NGSIConstants.RECV_TIME + "\", \"type\": \"timestamp\"},"
                + "{ \"id\": \"" + NGSIConstants.FIWARE_SERVICE_PATH + "\", \"type\": \"text\"},"
                + "{ \"id\": \"" + NGSIConstants.ENTITY_ID + "\", \"type\": \"text\"},"
                + "{ \"id\": \"" + NGSIConstants.ENTITY_TYPE + "\", \"type\": \"text\"},"
                + "{ \"id\": \"" + NGSIConstants.ATTR_NAME + "\", \"type\": \"text\"},"
                + "{ \"id\": \"" + NGSIConstants.ATTR_TYPE + "\", \"type\": \"text\"},"
                + "{ \"id\": \"" + NGSIConstants.ATTR_VALUE + "\", \"type\": \"json\"},"
                + "{ \"id\": \"" + NGSIConstants.ATTR_MD + "\", \"type\": \"json\"}"
                + "], "
                + "\"force\": \"true\" }";

        // create the CKAN request URL
        String urlPath = "/api/3/action/datastore_create";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

        // check the status
        if (res.getStatusCode() == 200) {
            System.out.println("Successful datastore creation (resourceId=" + resId + ")");
        } else {
            throw new Exception("Could not create the datastore (resId=" + resId
                    + ", statusCode=" + res.getStatusCode() + ")");
        } // if else
    } // createResource

    /**
     * Creates a datastore for a given resource in CKAN.
     * @param resId Identifies the resource whose datastore is going to be created.
     * @param records Array list with the attributes names for being used as fields with column mode
     * @throws Exception
     */
    private void createDataStoreWithFields(String resId, String records) throws Exception {
        // create the CKAN request JSON
        // CKAN types reference: http://docs.ckan.org/en/ckan-2.2/datastore.html#valid-types
        org.json.JSONObject jsonContent = new org.json.JSONObject(records);
        Iterator<String> keys = jsonContent.keys();
        ArrayList<String> fields = new ArrayList<>();
        Gson gson = new Gson();
        DataStore dataStore = new DataStore();
        ArrayList<JsonElement> jsonArray = new ArrayList<>();

        while (keys.hasNext()) {
            String key = keys.next();
            fields.add(key);
        }

        for (int i=0;i<fields.size();i++){
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("id",fields.get(i));
            jsonObject.addProperty("type","text");
            jsonArray.add((jsonObject.getAsJsonObject()));
        }

        dataStore.setResource_id(resId);
        dataStore.setFields(jsonArray);
        dataStore.setForce("true");
        String jsonString = gson.toJson(dataStore);

        System.out.println("Successful datastore creation jsonString=" + jsonString + "");

        // create the CKAN request URL
        String urlPath = "/api/3/action/datastore_create";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

        // check the status
        if (res.getStatusCode() == 200) {
            System.out.println("Successful datastore creation (resourceId=" + resId + ")");
        } else {
            throw new Exception("Could not create the datastore (resId=" + resId
                    + ", statusCode=" + res.getStatusCode() + ")");
        } // if else
    } // createResource

    /**
     * Creates a view for a given resource in CKAN.
     * @param resId Identifies the resource whose view is going to be created.
     * @throws Exception
     */
    private void createView(String resId) throws Exception {
        if (!existsView(resId)) {
            // create the CKAN request JSON
            String jsonString = "{ \"resource_id\": \"" + resId + "\","
                    + "\"view_type\": \"" + viewer + "\","
                    + "\"title\": \"Recline grid view\" }";

            // create the CKAN request URL
            String urlPath = "/api/3/action/resource_view_create";

            // do the CKAN request
            JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

            // check the status
            if (res.getStatusCode() == 200) {
                System.out.println("Successful view creation (resourceId=" + resId + ")");
            } else {
                throw new Exception("Could not create the datastore (resId=" + resId
                        + ", statusCode=" + res.getStatusCode() + ")");
            } // if else
        } // if
    } // createView

    private boolean existsView(String resId) throws Exception {
        // create the CKAN request JSON
        String jsonString = "{ \"id\": \"" + resId + "\" }";

        // create the CKAN request URL
        String urlPath = "/api/3/action/resource_view_list";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

        // check the status
        if (res.getStatusCode() == 200) {
            System.out.println("Successful view listing (resourceId=" + resId + ")");
            return (((JSONArray) res.getJsonObject().get("result")).size() > 0);
        } else {
            throw new Exception("Could not check if the view exists (resId=" + resId
                    + ", statusCode=" + res.getStatusCode() + ")");
        } // if else
    } // existsView

    private JSONObject getRecords(String resId, String filters, int offset, int limit)
            throws Exception {
        // create the CKAN request JSON
        String jsonString = "{\"id\": \"" + resId + "\",\"sort\":\"_id\",\"offset\":" + offset
                + ",\"limit\":" + limit;

        if (filters == null || filters.isEmpty()) {
            jsonString += "}";
        } else {
            jsonString += ",\"filters\":\"" + filters + "\"}";
        } // if else

        // create the CKAN request URL
        String urlPath = "/api/3/action/datastore_search";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

        // check the status
        if (res.getStatusCode() == 200) {
            System.out.println("Successful search (resourceId=" + resId + ")");
            return res.getJsonObject();
        } else {
            throw new Exception("Could not search for the records (resId=" + resId
                    + ", statusCode=" + res.getStatusCode() + ")");
        } // if else
    } // getRecords

    private void deleteRecords(String resId, String filters) throws Exception {
        // create the CKAN request JSON
        String jsonString = "{\"id\": \"" + resId + "\",\"force\":\"true\"";

        if (filters == null || filters.isEmpty()) {
            jsonString += "}";
        } else {
            jsonString += ",\"filters\":" + filters + "}";
        } // if else

        // create the CKAN request URL
        String urlPath = "/api/3/action/datastore_delete";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

        // check the status
        if (res.getStatusCode() == 200) {
            System.out.println("Successful deletion (resourceId=" + resId + ")");
        } else {
            throw new Exception("Could not delete the records (resId=" + resId
                    + ", statusCode=" + res.getStatusCode() + ")");
        } // if else
    } // deleteRecords

    public void capRecords(String orgName, String pkgName, String resName, long maxRecords)
            throws Exception {
        // Get the resource ID by querying the cache
        String resId = cache.getResId(orgName, pkgName, resName);

        // Create the filters for a datastore deletion
        String filters = "";

        // Get the record pages, some variables
        int offset = 0;
        long toBeDeleted = 0;
        long alreadyDeleted = 0;

        do {
            // Get the number of records to be deleted
            JSONObject result = (JSONObject) getRecords(resId, null, offset, RECORDSPERPAGE).get("result");
            long total = (Long) result.get("total");
            toBeDeleted = total - maxRecords;

            if (toBeDeleted < 0) {
                break;
            } // if

            // Get how much records within the current page must be deleted
            long remaining = toBeDeleted - alreadyDeleted;
            long toBeDeletedNow = (remaining > RECORDSPERPAGE ? RECORDSPERPAGE : remaining);

            // Get the records to be deleted from the current page and get their ID
            JSONArray records = (JSONArray) result.get("records");

            for (int i = 0; i < toBeDeletedNow; i++) {
                long id = (Long) ((JSONObject) records.get(i)).get("_id");

                if (filters.isEmpty()) {
                    filters += "{\"_id\":[" + id;
                } else {
                    filters += "," + id;
                } // if else
            } // for

            // Updates
            alreadyDeleted += toBeDeletedNow;
            offset += RECORDSPERPAGE;
        } while (alreadyDeleted < toBeDeleted);

        if (filters.isEmpty()) {
            System.out.println("No records to be deleted");
        } else {
            filters += "]}";
            System.out.println("Records must be deleted (resId=" + resId + ", filters=" + filters + ")");
            deleteRecords(resId, filters);
        } // if else
    } // capRecords

    public void expirateRecords(String orgName, String pkgName, String resName, long expirationTime)
            throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    } // expirateRecords

    public void expirateRecordsCache(long expirationTime) throws Exception {
        // Iterate on the cached resource IDs
        cache.startResIterator();

        while (cache.hasNextRes()) {
            // Get the next resource ID
            String resId = cache.getNextResId();

            // Create the filters for a datastore deletion
            String filters = "";

            // Get the record pages, some variables
            int offset = 0;
            boolean morePages = true;

            do {
                // Get the records within the current page
                JSONObject result = (JSONObject) getRecords(resId, null, offset, RECORDSPERPAGE).get("result");
                JSONArray records = (JSONArray) result.get("records");

                try {
                    for (Object recordObj : records) {
                        JSONObject record = (JSONObject) recordObj;
                        long id = (Long) record.get("_id");
                        String recvTime = (String) record.get("recvTime");
                        long recordTime = CommonConstants.getMilliseconds(recvTime);
                        long currentTime = new Date().getTime();

                        if (recordTime < (currentTime - (expirationTime * 1000))) {
                            if (filters.isEmpty()) {
                                filters += "{\"_id\":[" + id;
                            } else {
                                filters += "," + id;
                            } // if else
                        } else {
                            // Since records are sorted by _id, once the first not expirated record is found the loop
                            // can finish
                            morePages = false;
                            break;
                        } // if else
                    } // for
                } catch (ParseException e) {
                    throw new Exception("Data expiration error ParseException"+ e.getMessage());
                } // try catch

                if (records.isEmpty()) {
                    morePages = false;
                } else {
                    offset += RECORDSPERPAGE;
                } // if else
            } while (morePages);

            if (filters.isEmpty()) {
                System.out.println("No records to be deleted");
            } else {
                filters += "]}";
                System.out.println("Records to be deleted, resId=" + resId + ", filters=" + filters);
                deleteRecords(resId, filters);
            } // if else
        } // while
    } // expirateRecordsCache

    /**
     * Sets the CKAN cache. This is protected since it is only used by the tests.
     * @param cache
     */
    protected void setCache(CKANCache cache) {
        this.cache = cache;
    } // setCache

    private JsonResponse doCKANRequest(String method, String urlPath, String jsonString) throws Exception {
        ArrayList<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader("Authorization", apiKey));
        headers.add(new BasicHeader("Content-Type", "application/json; charset=utf-8"));
        return doRequest(method, urlPath, true, headers, new StringEntity(jsonString, "UTF-8"));
    } // doCKANRequest


    /**
     * Builds an organization name given a fiwareService. It throws an exception if the naming conventions are violated.
     * @param service
     * @return Organization name
     * @throws Exception
     */
    public String buildOrgName(String service, String dataModel, boolean enableEncoding, boolean enableLowercase, String ngsiVersion, DCATMetadata dcatMetadata) throws Exception {
        String orgName="";
        String fiwareService="";
        if (dcatMetadata!=null){
            if (dcatMetadata.getOrganizationName()!=null){
                fiwareService=(enableLowercase)?service.toLowerCase():dcatMetadata.getOrganizationName();
            }else{
                fiwareService=(enableLowercase)?service.toLowerCase():service;
            }
        }else{
            fiwareService=(enableLowercase)?service.toLowerCase():service;
        }

        if ("v2".equals(ngsiVersion)){

        }
        else if ("ld".equals(ngsiVersion)) {
            switch (dataModel) {
                case "db-by-entity-id":
                    //FIXME
                    //note that if we enable encode() and/or encodeCKAN() in this datamodel we could have problems, although it need to be analyzed in deep
                    orgName = NGSICharsets.encodeCKAN(fiwareService);
                    break;
                case "db-by-entity":
                    if (enableEncoding) {
                        orgName = NGSICharsets.encodeCKAN(fiwareService);
                    } else {
                        orgName = NGSICharsets.encode(fiwareService, false, true).toLowerCase(Locale.ENGLISH);
                    } // if else

                    if (orgName.length() > NGSIConstants.CKAN_MAX_NAME_LEN) {
                        throw new Exception("Building organization name '" + orgName + "' and its length is "
                                + "greater than " + NGSIConstants.CKAN_MAX_NAME_LEN);
                    } else if (orgName.length() < NGSIConstants.CKAN_MIN_NAME_LEN) {
                        throw new Exception("Building organization name '" + orgName + "' and its length is "
                                + "lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);
                    } // if else if
                    break;
                default:
                    throw new Exception("Not supported Data Model for CKAN Sink: " + dataModel);
            }
        }
        return orgName;
    } // buildOrgName

    /**
     * Builds a package name given a fiwareService and a fiwareServicePath. It throws an exception if the naming
     * conventions are violated.
     * @param fiwareService
     * @return Package name
     * @throws Exception
     */
    public String buildPkgName( String fiwareService, Entity entity, String dataModel, boolean enableEncoding, boolean enableLowercase, String ngsiVersion,DCATMetadata dcatMetadata) throws Exception {
        String pkgName="";
        String entityId = "";

        if (dcatMetadata!=null){
            if (dcatMetadata.getPackageName()!=null){
                fiwareService=(enableLowercase)? entity.getEntityId().toLowerCase() :dcatMetadata.getPackageName();
                entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() :dcatMetadata.getPackageName();
            }else{
                entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
            }
        }else{
            entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
        }

        if ("v2".equals(ngsiVersion)){

        }
        else if ("ld".equals(ngsiVersion)) {
            switch (dataModel) {
                case "db-by-entity-id":
                    //FIXME
                    //note that if we enable encode() and/or encodeCKAN() in this datamodel we could have problems, although it need to be analyzed in deep
                    pkgName = NGSICharsets.encodeCKAN(entityId);
                    break;
                case "db-by-entity":
                    if (enableEncoding) {
                        pkgName = NGSICharsets.encodeCKAN(entityId);

                    } else {
                        pkgName = NGSICharsets.encode(entityId, false, true).toLowerCase(Locale.ENGLISH);
                    } // if else
                    if (pkgName.length() > NGSIConstants.CKAN_MAX_NAME_LEN) {
                        throw new Exception("Building package name '" + pkgName + "' and its length is "
                                + "greater than " + NGSIConstants.CKAN_MAX_NAME_LEN);
                    } else if (pkgName.length() < NGSIConstants.CKAN_MIN_NAME_LEN) {
                        throw new Exception("Building package name '" + pkgName + "' and its length is "
                                + "lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);
                    } // if else if
                    break;
                default:
                    throw new Exception("Not supported Data Model for CKAN Sink: " + dataModel);
            }
        }
        return pkgName;
    } // buildPkgName

    /**
     * Builds a resource name given a entity. It throws an exception if the naming conventions are violated.
     * @param entity
     * @return Resource name
     * @throws Exception
     */
    public String buildResName(Entity entity, String dataModel, boolean enableEncoding, boolean enableLowercase, String ngsiVersion, DCATMetadata dcatMetadata) throws Exception {
        String resName = "";
        String entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
        String entityType = (enableLowercase) ? entity.getEntityType().toLowerCase() : entity.getEntityType();
        if ("v2".equals(ngsiVersion)) {

        } else if ("ld".equals(ngsiVersion)) {
            if (dcatMetadata != null && dcatMetadata.getResourceName() != null) {
                resName = (enableLowercase) ? entity.getEntityId().toLowerCase() : dcatMetadata.getResourceName();

            } else {
                entityId = (enableLowercase) ? entity.getEntityId().toLowerCase() : entity.getEntityId();
                switch (dataModel) {
                    case "db-by-entity-id":
                        //FIXME
                        //note that if we enable encode() and/or encodeCKAN() in this datamodel we could have problems, although it need to be analyzed in deep
                        resName = entityId;
                        break;
                    case "db-by-entity":
                        if (enableEncoding) {
                            resName = NGSICharsets.encodeCKAN(entityId) + "_" + NGSICharsets.encodeCKAN(entityType);
                        } else {
                            resName = NGSICharsets.encode(entityId, false, true).toLowerCase(Locale.ENGLISH) + "_" + NGSICharsets.encode(entityType, false, true);
                        } // if else

                        if (resName.length() > NGSIConstants.CKAN_MAX_NAME_LEN) {
                            throw new Exception("Building resource name '" + resName + "' and its length is "
                                    + "greater than " + NGSIConstants.CKAN_MAX_NAME_LEN);
                        } else if (resName.length() < NGSIConstants.CKAN_MIN_NAME_LEN) {
                            throw new Exception("Building resource name '" + resName + "' and its length is "
                                    + "lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);
                        } // if else if
                        break;
                    default:
                        throw new Exception("Not supported Data Model for CKAN Sink: " + dataModel);
                }
            }
        }
        return resName;
    } // buildResName


    public boolean isValid(String test) {
        try {
            new org.json.JSONObject(test);
        } catch (JSONException ex) {
            try {
                new org.json.JSONArray(test);
            } catch (JSONException ex1) {
                return false;
            }
        }
        return true;
    }

}
