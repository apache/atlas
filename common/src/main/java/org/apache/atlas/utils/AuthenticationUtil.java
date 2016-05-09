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

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Util class for Authentication.
 */
public final class AuthenticationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationUtil.class);

    private AuthenticationUtil() {
    }

    public static boolean isKerberosAuthicationEnabled() {
        boolean isKerberosAuthicationEnabled = false;
        try {
            Configuration atlasConf = ApplicationProperties.get();

            if ("true".equalsIgnoreCase(atlasConf.getString("atlas.http.authentication.enabled"))
                    && "kerberos".equalsIgnoreCase(atlasConf.getString("atlas.http.authentication.type"))) {
                isKerberosAuthicationEnabled = true;
            } else {
                isKerberosAuthicationEnabled = false;
            }

        } catch (AtlasException e) {
            LOG.error("Error while isKerberosAuthicationEnabled ", e);
        }
        return isKerberosAuthicationEnabled;
    }

    public static String[] getBasicAuthenticationInput() {
        String username = null;
        String password = null;

        try {
            BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter username for atlas :-");
            username = bufferRead.readLine();
            System.out.println("Enter password for atlas :-");
            password = bufferRead.readLine();
        } catch (Exception e) {
            System.out.print("Error while reading ");
            System.exit(1);
        }
        return new String[]{username, password};
    }

}
