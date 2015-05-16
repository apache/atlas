package org.apache.atlas.regression.security;

import org.apache.hadoop.security.UserGroupInformation;
import org.testng.Assert;

import java.io.IOException;
import java.util.HashMap;

public class KerberosHelper {

    private KerberosHelper() {
        throw new AssertionError("Instantiating utility class...");
    }

    // determine if running on a secure cluster if secure=true is sent
    public static final boolean IS_SECURE =  Boolean.parseBoolean(System.getProperty("secure",
            "false"));

    /** keytab of current user. */
    private static final String CURRENT_USER_KEYTAB = System.getProperty("current.user.keytab",
            null);

    // determine the user realm to use
    private static final String USER_REALM = System.getProperty("user.realm", "");

    private static HashMap<String, String> keyTabMap;

    /* initialize keyTabMap */
    static {
        keyTabMap = new HashMap<>();
        keyTabMap.put(System.getProperty("user.name"), CURRENT_USER_KEYTAB);
    }

    public static UserGroupInformation getUGI(String user) throws IOException {
        // if unsecure cluster create a remote user object
        if (!IS_SECURE) {
            return UserGroupInformation.createRemoteUser(user);
        }
        // if secure create a ugi object from keytab
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(getPrincipal(user),
                getKeytabForUser(user));
    }

    private static String getPrincipal(String user) {
        return USER_REALM.isEmpty() ? user : user + '@' + USER_REALM;
    }

    private static String getKeytabForUser(String user) {
        Assert.assertTrue(keyTabMap.containsKey(user), "Unknown user: " + user);
        return keyTabMap.get(user);
    }
}