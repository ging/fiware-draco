package org.apache.nifi.processors.ngsi.NGSI.backends.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class HttpBackend {
    private final LinkedList<String> hosts;
    private final String port;
    private final boolean ssl;
    private final boolean krb5;
    private final String krb5User;
    private final String krb5Password;
    private final HttpClientFactory httpClientFactory;
    private HttpClient httpClient;

    /**
     * Constructor.
     * @param hosts
     * @param port
     * @param ssl
     * @param krb5
     * @param krb5User
     * @param krb5Password
     * @param krb5LoginConfFile
     * @param krb5ConfFile
     */
    public HttpBackend(String[] hosts, String port, boolean ssl, boolean krb5, String krb5User, String krb5Password,
                       String krb5LoginConfFile, String krb5ConfFile) {
        this.hosts = new LinkedList(Arrays.asList(hosts));
        this.port = port;
        this.ssl = ssl;
        this.krb5 = krb5;
        this.krb5User = krb5User;
        this.krb5Password = krb5Password;

        // create a Http clients factory and an initial connection
        httpClientFactory = new HttpClientFactory(ssl, krb5LoginConfFile, krb5ConfFile);
        httpClient = httpClientFactory.getHttpClient(ssl, krb5);
    } // HttpBackend

    /**
     * Sets the http client.
     * @param httpClient
     */
    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    } // setHttpClient

    /**
     * @param method
     * @param url
     * @param relative
     * @param headers
     * @param entity
     * @return A Http httpRes
     * @throws Exception
     */
    public JsonResponse doRequest(String method, String url, boolean relative, ArrayList<Header> headers,
                                  StringEntity entity) throws Exception {
        JsonResponse response;

        if (relative) {
            // iterate on the hosts
            for (String host : hosts) {
                // create the HttpFS URL
                String effectiveURL = (ssl ? "https://" : "http://") + host + ":" + port + url;

                try {
                    if (krb5) {
                        response = doPrivilegedRequest(method, effectiveURL, headers, entity);
                    } else {
                        response = doRequest(method, effectiveURL, headers, entity);
                    } // if else
                } catch (Exception e) {
                    System.out.println("There was a problem when performing the request (details=" + e.getMessage() + "). "
                            + "Most probably the used http endpoint (" + host + ") is not active, trying another one");
                    continue;
                } // try catch

                // place the current host in the first place (if not yet placed), since it is currently working
                if (!hosts.getFirst().equals(host)) {
                    hosts.remove(host);
                    hosts.add(0, host);
                    System.out.println("Placing the host in the first place of the list (host=" + host + ")");
                } // if

                return response;
            } // for

            throw new Exception("No http endpoint was reachable");
        } else {
            if (krb5) {
                return doPrivilegedRequest(method, url, headers, entity);
            } else {
                return doRequest(method, url, headers, entity);
            } // if else
        } // if else
    } // doRequest

    /**

     *
     * @param method
     * @param url
     * @param headers
     * @param entity
     * @return
     * @throws java.lang.Exception
     */

    protected JsonResponse doRequest(String method, String url, ArrayList<Header> headers, StringEntity entity)
            throws Exception {
        HttpResponse httpRes = null;
        HttpRequestBase request = null;

        if (method.equals("PUT")) {
            HttpPut req = new HttpPut(url);

            if (entity != null) {
                req.setEntity(entity);
            } // if

            request = req;
        } else if (method.equals("POST")) {
            HttpPost req = new HttpPost(url);

            if (entity != null) {
                req.setEntity(entity);
            } // if

            request = req;
        } else if (method.equals("GET")) {
            request = new HttpGet(url);
        } else if (method.equals("DELETE")) {
            request = new HttpDelete(url);
        } else {
            throw new Exception("HTTP method not supported: " + method);
        } // if else

        if (headers != null) {
            for (Header header : headers) {
                request.setHeader(header);
            } // for
        } // if

        System.out.println("Http request: " + request.toString());

        try {
            httpRes = httpClient.execute(request);
        } catch (IOException e) {
            request.releaseConnection();
            throw new Exception(e.getMessage());
        } // try catch

        JsonResponse response = createJsonResponse(httpRes);
        request.releaseConnection();
        return response;
    } // doRequest

    private JsonResponse doPrivilegedRequest(String method, String url, ArrayList<Header> headers,
                                             StringEntity entity) throws Exception {
        try {
            LoginContext loginContext = new LoginContext("Draco_krb5_login",
                    new KerberosCallbackHandler(krb5User, krb5Password));
            loginContext.login();
            PrivilegedRequest req = new PrivilegedRequest(method, url, headers, entity);
            return createJsonResponse((HttpResponse) Subject.doAs(loginContext.getSubject(), req));
        } catch (LoginException e) {
            System.out.println(e.getMessage());
            return null;
        } // try catch // try catch
    } // doPrivilegedRequest

    /**
     * PrivilegedRequest class.
     */
    private class PrivilegedRequest implements PrivilegedAction {

        private final String method;
        private final String url;
        private final ArrayList<Header> headers;
        private final StringEntity entity;

        /**
         * Constructor.
         * @param url
         * @param headers
         * @param entity
         */
        public PrivilegedRequest(String method, String url, ArrayList<Header> headers, StringEntity entity) {

            this.method = method;
            this.url = url;
            this.headers = headers;
            this.entity = entity;
        } // PrivilegedRequest

        @Override
        public Object run() {
            try {
                Subject current = Subject.getSubject(AccessController.getContext());
                Set<Principal> principals = current.getPrincipals();

                for (Principal next : principals) {
                    System.out.println("DOAS Principal: " + next.getName());
                } // for

                return doRequest(method, url, headers, entity);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                return null;
            } // try catch
        } // run

    } // PrivilegedRequest

    private JsonResponse createJsonResponse(HttpResponse httpRes) throws Exception {
        try {
            if (httpRes == null) {
                return null;
            } // if

            System.out.println("Http response status line: " + httpRes.getStatusLine().toString());

            // parse the httpRes payload
            JSONObject jsonPayload = null;
            HttpEntity entity = httpRes.getEntity();

            if (entity != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(httpRes.getEntity().getContent()));
                String res = "";
                String line;

                while ((line = reader.readLine()) != null) {
                    res += line;
                } // while

                reader.close();
                System.out.println("Http response payload: " + res);

                if (!res.isEmpty()) {
                    JSONParser jsonParser = new JSONParser();

                    if (res.startsWith("[")) {
                        Object object = jsonParser.parse(res);
                        jsonPayload = new JSONObject();
                        jsonPayload.put("result", (JSONArray) object);
                    } else {
                        jsonPayload = (JSONObject) jsonParser.parse(res);
                    } // if else
                } // if
            } // if

            // get the location header
            Header locationHeader = null;
            Header[] headers = httpRes.getHeaders("Location");

            if (headers.length > 0) {
                locationHeader = headers[0];
            } // if

            // return the result
            return new JsonResponse(jsonPayload, httpRes.getStatusLine().getStatusCode(),
                    httpRes.getStatusLine().getReasonPhrase(), locationHeader);
        } catch (IOException e) {
            throw new Exception(e.getMessage());
        } catch (IllegalStateException e) {
            throw new Exception(e.getMessage());
        } // try catch
    } // createJsonResponse
}
