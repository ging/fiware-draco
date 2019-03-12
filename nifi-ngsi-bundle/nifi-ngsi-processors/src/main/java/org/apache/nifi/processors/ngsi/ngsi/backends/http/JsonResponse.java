package org.apache.nifi.processors.ngsi.ngsi.backends.http;

import org.apache.http.Header;
import org.json.simple.JSONObject;

public class JsonResponse {


    private final JSONObject jsonObject;
    private final int statusCode;
    private final String reasonPhrase;
    private final Header locationHeader;

    /**
     * Constructor.
     * @param jsonObject
     * @param statusCode
     * @param reasonPhrase
     * @param locationHeader
     */
    public JsonResponse(JSONObject jsonObject, int statusCode, String reasonPhrase, Header locationHeader) {
        this.jsonObject = jsonObject;
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
        this.locationHeader = locationHeader;
    } // JsonResponse

    /**
     * Gets the Json object.
     * @return jsonObject
     */
    public JSONObject getJsonObject() {
        return jsonObject;
    } // getJsonObject

    /**
     * Gets the status code.
     * @return statusCode
     */
    public int getStatusCode() {
        return statusCode;
    } // getStatusCode

    /**
     * Gets the reason phrase.
     * @return reasonPhrase
     */
    public String getReasonPhrase() {
        return reasonPhrase;
    } // getReasonPhrase

    /**
     * Gets the location header.
     * @return locationHeader
     */
    public Header getLocationHeader() {
        return locationHeader;
    } // getLocationHeader

}
