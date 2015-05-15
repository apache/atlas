package org.apache.atlas.regression.security;

import org.apache.atlas.regression.request.BaseRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.http.auth.AuthenticationException;
import org.apache.log4j.Logger;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ConcurrentHashMap;

public class AtlasAuthenticationToken {
    private static final String AUTH_URL = "api/options";
    private static final KerberosAuthenticator AUTHENTICATOR = new KerberosAuthenticator();
    private static final AtlasAuthenticationToken INSTANCE = new AtlasAuthenticationToken();
    private static final Logger LOGGER = Logger.getLogger(AtlasAuthenticationToken.class);

    // Use a hashmap so that we can cache the tokens.
    private final ConcurrentHashMap<String, AuthenticatedURL.Token> tokens =
            new ConcurrentHashMap<>();

    private AtlasAuthenticationToken() {
    }

    public static final HostnameVerifier ALL_TRUSTING_HOSTNAME_VERIFIER = new HostnameVerifier() {
        @Override
        public boolean verify(String hostname, SSLSession sslSession) {
            return true;
        }
    };

    private static void authenticate(String user, String protocol, String host, int port)
            throws IOException, AuthenticationException, InterruptedException {
        final URL url = new URL(String.format("%s://%s:%d/%s", protocol, host, port,
                AUTH_URL + "?" + PseudoAuthenticator.USER_NAME + "=" + user));
        LOGGER.info("Authorize using url: " + url.toString());

        final AuthenticatedURL.Token currentToken = new AuthenticatedURL.Token();

        /*using KerberosAuthenticator which falls back to PsuedoAuthenticator
        instead of passing authentication type from the command line - bad factory*/
        try {
            HttpsURLConnection.setDefaultSSLSocketFactory(BaseRequest.getSslContext()
                    .getSocketFactory());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        HttpsURLConnection.setDefaultHostnameVerifier(ALL_TRUSTING_HOSTNAME_VERIFIER);
        UserGroupInformation callerUGI = KerberosHelper.getUGI(user);
        callerUGI.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                new AuthenticatedURL(AUTHENTICATOR).openConnection(url, currentToken);
                return null;
            }
        });
        String key = getKey(user, protocol, host, port);

        // initialize a hash map if its null.
        LOGGER.info("Authorization Token: " + currentToken.toString());
        INSTANCE.tokens.put(key, currentToken);
    }

    public static AuthenticatedURL.Token getToken(String user, String protocol, String host,
                                                  int port, boolean overWrite)
            throws IOException, AuthenticationException, InterruptedException {
        String key = getKey(user, protocol, host, port);

        /*if the tokens are null or if token is not found then we will go ahead and authenticate
        or if we are asked to overwrite*/
        if (!INSTANCE.tokens.containsKey(key) || overWrite) {
            authenticate(user, protocol, host, port);
        }
        return INSTANCE.tokens.get(key);
    }

    public static AuthenticatedURL.Token getToken(String user, String protocol, String host,
                                                  int port)
            throws IOException, AuthenticationException, InterruptedException {
        return getToken(user, protocol, host, port, false);
    }

    /*spnego token will be unique to the user and uri its being requested for.
    Hence the key of the hash map is the combination of user, protocol, host and port.*/
    private static String getKey(String user, String protocol, String host, int port) {
        return String.format("%s-%s-%s-%d", user, protocol, host, port);
    }
}
