/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.utils;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Console;

/**
 * Util class for Authentication.
 */
public final class AuthenticationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationUtil.class);

    private AuthenticationUtil() {
    }

    public static boolean isKerberosAuthenticationEnabled() {
        boolean isKerberosAuthenticationEnabled = false;
        try {
            isKerberosAuthenticationEnabled = isKerberosAuthenticationEnabled(ApplicationProperties.get());
        } catch (AtlasException e) {
            LOG.error("Error while isKerberosAuthenticationEnabled ", e);
        }
        return isKerberosAuthenticationEnabled;
    }

    public static boolean isKerberosAuthenticationEnabled(Configuration atlasConf) {
        return atlasConf.getBoolean("atlas.authentication.method.kerberos", false);
    }

    public static String[] getBasicAuthenticationInput() {
        String username = null;
        String password = null;

        try {
            Console console = System.console();
            username = console.readLine("Enter username for atlas :- ");
            password = new String(console.readPassword("Enter password for atlas :- "));
        } catch (Exception e) {
            System.out.print("Error while reading ");
            System.exit(1);
        }
        return new String[]{username, password};
    }

}
