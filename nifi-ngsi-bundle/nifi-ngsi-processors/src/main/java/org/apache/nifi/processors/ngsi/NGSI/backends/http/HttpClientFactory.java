package org.apache.nifi.processors.ngsi.NGSI.backends.http;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.nifi.processors.ngsi.NGSI.utils.CommonConstants;


public class HttpClientFactory {

    private final String loginConfFile;
    private final String krb5ConfFile;
    private static PoolingClientConnectionManager connectionsManager;
    private static PoolingClientConnectionManager sslConnectionsManager;

    /**
     * Constructor.
     * @param ssl True if SSL connections are desired. False otherwise.
     * @param loginConfFile
     * @param krb5ConfFile
     */
    public HttpClientFactory(boolean ssl, String loginConfFile, String krb5ConfFile) {
        // set the Kerberos parameters
        this.loginConfFile = loginConfFile;
        this.krb5ConfFile = krb5ConfFile;

        // create the appropriate connections manager
        if (ssl) {
            sslConnectionsManager = new PoolingClientConnectionManager(getSSLSchemeRegistry());
            sslConnectionsManager.setMaxTotal(CommonConstants.MAX_CONNS);
            sslConnectionsManager.setDefaultMaxPerRoute(CommonConstants.MAX_CONNS_PER_ROUTE);
        } else {
            connectionsManager = new PoolingClientConnectionManager();
            connectionsManager.setMaxTotal(CommonConstants.MAX_CONNS);
            connectionsManager.setDefaultMaxPerRoute(CommonConstants.MAX_CONNS_PER_ROUTE);
        } // if else

        System.out.println("Setting max total connections (" + CommonConstants.MAX_CONNS + ")");
        System.out.println("Setting default max connections per route (" + CommonConstants.MAX_CONNS_PER_ROUTE + ")");
    } // HttpClientFactory

    /**
     * Gets a HTTP client.
     * @param ssl True if SSL connections are desired. False otherwise
     * @param krb5Auth
     * @return A http client obtained from the (SSL) Connections Manager.
     */
    public DefaultHttpClient getHttpClient(boolean ssl, boolean krb5Auth) {
        DefaultHttpClient httpClient;

        if (ssl) {
            httpClient = new DefaultHttpClient(sslConnectionsManager);
        } else {
            httpClient = new DefaultHttpClient(connectionsManager);
        } // if else

        if (krb5Auth) {
            System.setProperty("java.security.auth.login.config", loginConfFile);
            System.setProperty("java.security.krb5.conf", krb5ConfFile);
            System.setProperty("sun.security.krb5.debug", "false");
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            Credentials jaasCredentials = new Credentials() {

                @Override
                public String getPassword() {
                    return null;
                } // getPassword

                @Override
                public Principal getUserPrincipal() {
                    return null;
                } // getUserPrincipal

            };

            SPNegoSchemeFactory spnegoSchemeFactory = new SPNegoSchemeFactory(true);
            httpClient.getAuthSchemes().register(AuthPolicy.SPNEGO, spnegoSchemeFactory);
            httpClient.getCredentialsProvider().setCredentials(new AuthScope(null, -1, null), jaasCredentials);
        } // if

        return httpClient;
    } // getHttpClient

    /**
     * @param ssl
     * @return
     */
    public int getLeasedConnections(boolean ssl) {
        if (ssl) {
            return sslConnectionsManager.getTotalStats().getLeased();
        } else {
            return connectionsManager.getTotalStats().getLeased();
        } // if else
    } // getLeasedConnections

    /**
     * * @return A SSL SchemeRegistry object.
     */
    private SchemeRegistry getSSLSchemeRegistry() {

        SSLContext sslContext;

        try {
            sslContext = SSLContext.getInstance("SSL");
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Fatal error (SSL cannot be used, no such algorithm. Details=" + e.getMessage() + ")");
            return null;
        } // try catch // try catch

        try {
            // set up a TrustManager that trusts everything
            sslContext.init(null, new TrustManager[] {
                    new X509TrustManager() {
                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        } // getAcceptedIssuers

                        @Override
                        public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        } // getAcceptedIssuers

                        @Override
                        public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        } // checkServerTrusted
                    }
            }, new SecureRandom());
        } catch (KeyManagementException e) {
            System.out.println("Fatal error (Cannot ignore SSL certificates. Details=" + e.getMessage() + ")");
            return null;
        } // try catch // try catch

        if (sslContext == null) {
            System.out.println("Fatal error (Cannot ignore SSL certificates, SSL context is null)");
            return null;
        } // if

        SSLSocketFactory sf = new SSLSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        Scheme httpsScheme = new Scheme("https", 443, sf);
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(httpsScheme);
        return schemeRegistry;
    } // getSSLSchemeRegistry

} // HttpClientFactory
