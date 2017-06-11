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

package org.apache.atlas.examples;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.configuration.Configuration;

import java.io.Console;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * A driver that sets up types supplied in a file.  
 */
public class UpdateTypesFromJsonFileUtil extends AtlasBaseClient{
    public static final String ATLAS_REST_ADDRESS = "atlas.rest.address";

    public static void main(String[] args) throws Exception {

        Console console = System.console();
        if (console == null) {
            System.err.println("No console.");
            System.exit(1);
        }
        String[] basicAuthUsernamePassword = null;
        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();
        }
        AtlasClientV2 atlasClientV2 = getAtlasClientV2(args, basicAuthUsernamePassword);


        String createFileName = console.readLine("Enter fileName containing TypeDefs for create:- ");
        File createFile = new File(createFileName);
        String createJsonStr = new String( Files.readAllBytes(createFile.toPath()), StandardCharsets.UTF_8);

        System.err.println("create json is :\n" + createJsonStr);

        runTypeCreation(createJsonStr,atlasClientV2);
//        String updateFileName = console.readLine("Enter fileName containing TypeDefs for update:- ");
//        File updateFile = new File(updateFileName);
//        String updateJsonStr = new String( Files.readAllBytes(updateFile.toPath()), StandardCharsets.UTF_8);
//        System.err.println("update json is :\n" + updateJsonStr);
//        runTypeUpdate(updateJsonStr,atlasClientV2);
    }

    @VisibleForTesting
    static void runTypeCreation(String jsonStr,AtlasClientV2 atlasClientV2) throws Exception {
        AtlasTypesDef typesDef = AtlasType.fromJson(jsonStr, AtlasTypesDef.class);
        atlasClientV2.createAtlasTypeDefs(typesDef);
    }
    @VisibleForTesting
    static void runTypeUpdate(String jsonStr,AtlasClientV2 atlasClientV2) throws Exception {
        AtlasTypesDef typesDef = AtlasType.fromJson(jsonStr, AtlasTypesDef.class);
        atlasClientV2.updateAtlasTypeDefs(typesDef);
    }

    private static AtlasClientV2 getAtlasClientV2(String[] args, String[] basicAuthUsernamePassword) throws AtlasException {
        String[] urls = getServerUrl(args);

        AtlasClientV2 atlasClientV2;

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            atlasClientV2 =new AtlasClientV2(urls,basicAuthUsernamePassword);
        } else {
            atlasClientV2 = new AtlasClientV2(urls);
        }
        return atlasClientV2;
    }

    static String[] getServerUrl(String[] args) throws AtlasException {
        if (args.length > 0) {
            return args[0].split(",");
        }

        Configuration configuration = ApplicationProperties.get();
        String[] urls = configuration.getStringArray(ATLAS_REST_ADDRESS);
        if (urls == null || urls.length == 0) {
            System.out.println("Usage: quick_start.py <atlas endpoint of format <http/https>://<atlas-fqdn>:<atlas port> like http://localhost:21000>");
            System.exit(-1);
        }

        return urls;
    }
}