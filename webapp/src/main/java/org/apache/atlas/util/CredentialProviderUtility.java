/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.atlas.security.SecurityProperties.KEYSTORE_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityProperties.SERVER_CERT_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityProperties.TRUSTSTORE_PASSWORD_KEY;

/**
 * A utility class for generating a credential provider containing the entries required for supporting the SSL
 * implementation
 * of the DGC server.
 */
public class CredentialProviderUtility {
    private static final String[] KEYS =
            new String[]{KEYSTORE_PASSWORD_KEY, TRUSTSTORE_PASSWORD_KEY, SERVER_CERT_PASSWORD_KEY};

    public static abstract class TextDevice {
        public abstract void printf(String fmt, Object... params);

        public abstract String readLine(String fmt, Object... args);

        public abstract char[] readPassword(String fmt, Object... args);

    }

    private static TextDevice DEFAULT_TEXT_DEVICE = new TextDevice() {
        Console console = System.console();

        @Override
        public void printf(String fmt, Object... params) {
            console.printf(fmt, params);
        }

        @Override
        public String readLine(String fmt, Object... args) {
            return console.readLine(fmt, args);
        }

        @Override
        public char[] readPassword(String fmt, Object... args) {
            return console.readPassword(fmt, args);
        }
    };

    public static TextDevice textDevice = DEFAULT_TEXT_DEVICE;

    public static void main(String[] args) throws IOException {
        // prompt for the provider name
        CredentialProvider provider = getCredentialProvider(textDevice);

        if(provider != null) {
            char[] cred;
            for (String key : KEYS) {
                cred = getPassword(textDevice, key);
                // create a credential entry and store it
                boolean overwrite = true;
                if (provider.getCredentialEntry(key) != null) {
                    String choice = textDevice.readLine("Entry for %s already exists.  Overwrite? (y/n) [y]:", key);
                    overwrite = StringUtils.isEmpty(choice) || choice.equalsIgnoreCase("y");
                    if (overwrite) {
                        provider.deleteCredentialEntry(key);
                        provider.flush();
                        provider.createCredentialEntry(key, cred);
                        provider.flush();
                        textDevice.printf("Entry for %s was overwritten with the new value.\n", key);
                    } else {
                        textDevice.printf("Entry for %s was not overwritten.\n", key);
                    }
                } else {
                    provider.createCredentialEntry(key, cred);
                    provider.flush();
                }
            }
        }
    }

    /**
     * Retrieves a password from the command line.
     * @param textDevice  the system console.
     * @param key   the password key/alias.
     * @return the password.
     */
    private static char[] getPassword(TextDevice textDevice, String key) {
        boolean noMatch;
        char[] cred = new char[0];
        char[] passwd1;
        char[] passwd2;
        do {
            passwd1 = textDevice.readPassword("Please enter the password value for %s:", key);
            passwd2 = textDevice.readPassword("Please enter the password value for %s again:", key);
            noMatch = !Arrays.equals(passwd1, passwd2);
            if (noMatch) {
                if (passwd1 != null) {
                    Arrays.fill(passwd1, ' ');
                }
                textDevice.printf("Password entries don't match. Please try again.\n");
            } else {
                if (passwd1.length == 0) {
                    textDevice.printf("An empty password is not valid.  Please try again.\n");
                    noMatch = true;
                } else {
                    cred = passwd1;
                }
            }
            if (passwd2 != null) {
                Arrays.fill(passwd2, ' ');
            }
        } while (noMatch);
        return cred;
    }

    /**\
     * Returns a credential provider for the entered JKS path.
     * @param textDevice the system console.
     * @return the Credential provider
     * @throws IOException
     */
    private static CredentialProvider getCredentialProvider(TextDevice textDevice) throws IOException {
        String providerPath = textDevice.readLine("Please enter the full path to the credential provider:");

        if (providerPath != null) {
            Configuration conf = new Configuration(false);
            conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath);
            return CredentialProviderFactory.getProviders(conf).get(0);
        }

        return null;
    }
}
