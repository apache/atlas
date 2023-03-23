/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.keycloak.client;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY;

public class KeycloakClient {
    public static final Logger LOG = LoggerFactory.getLogger(KeycloakClient.class);

    private static KeycloakClient keycloakClient = null;
    public static Keycloak keycloak = null;

    public static final String KEYCLOAK_PROPERTIES = "keycloak.json";

    private static String REALM_ID;
    private static String AUTH_SERVER_URL;
    private static String CLIENT_ID;
    private static String CLIENT_SECRET;
    private static String GRANT_TYPE;


    private KeycloakClient() {
    }

    public static KeycloakClient getKeycloakClient() throws AtlasBaseException {
        if (keycloakClient == null) {
            LOG.info("Initializing Keycloak client..");
            try {
                initConf();
            } catch (IOException e) {
                LOG.error("Failed to fetch Keycloak conf {}", e.getMessage());
                throw new AtlasBaseException(e);
            } catch (JSONException e) {
                LOG.error("Failed to parsing Keycloak conf {}", e.getMessage());
                throw new AtlasBaseException(e);
            } catch (Exception e) {
                LOG.error("Failed to connect to Keycloak {}", e.getMessage());
                throw new AtlasBaseException(e);
            }
            init();
            LOG.info("Initialized Keycloak client..");
        }

        if (keycloak.isClosed()) {
            LOG.info("Re-initializing keycloak client");
            init();
        }

        return keycloakClient;
    }

    private static void initConf() throws Exception {
        String confLocation = System.getProperty(ATLAS_CONFIGURATION_DIRECTORY_PROPERTY);

        File confFile;
        if (StringUtils.isNotEmpty(confLocation)) {
            confFile = new File(confLocation, KEYCLOAK_PROPERTIES);

            if (confFile.exists()) {
                String keyConf = new String(Files.readAllBytes(confFile.toPath()), StandardCharsets.UTF_8);

                JSONObject object = new JSONObject(keyConf);

                REALM_ID = object.getString("realm");
                AUTH_SERVER_URL = object.getString("auth-server-url");
                CLIENT_ID = object.getString("resource");
                GRANT_TYPE = "client_credentials";
                CLIENT_SECRET = object.getJSONObject("credentials").getString("secret");

                LOG.info("Keycloak conf: REALM_ID:{}, AUTH_SERVER_URL:{}",
                        REALM_ID, AUTH_SERVER_URL);
            } else {
                throw new AtlasBaseException("Keycloak configuration file not found in location " + confLocation);
            }
        }
    }

    private static void init() {
        synchronized (KeycloakClient.class) {
            if (keycloakClient == null) {
                keycloak = KeycloakBuilder.builder()
                        .serverUrl(AUTH_SERVER_URL)
                        .realm(REALM_ID)
                        .clientId(CLIENT_ID)
                        .clientSecret(CLIENT_SECRET)
                        .grantType(GRANT_TYPE)
                        .resteasyClient(new ResteasyClientBuilder().build())
                        //.resteasyClient(ClientBuilder.newBuilder().build())
                        .build();

                keycloakClient = new KeycloakClient();
            }
        }
    }

    public RealmResource getRealm() {
        return keycloak.realm(REALM_ID);
    }

    public List<UserRepresentation> getAllUsers() {
        int start = 0;
        int size = 100;

        List<UserRepresentation> ret = new ArrayList<>();

        do {
            List<UserRepresentation> userRepresentations = getRealm().users().list(start, size);
            ret.addAll(userRepresentations);
            start += size;

        } while (CollectionUtils.isNotEmpty(ret) && ret.size() % size == 0);

        return ret;
    }

    public List<GroupRepresentation> getAllGroups() {

        int start = 0;
        int size = 100;

        List<GroupRepresentation> ret = new ArrayList<>();

        do {
            List<GroupRepresentation> groupRepresentations = getRealm().groups().groups(start, size);
            ret.addAll(groupRepresentations);
            start += size;

        } while (CollectionUtils.isNotEmpty(ret) && ret.size() % size == 0);

        return ret;
    }

    public List<RoleRepresentation> getAllRoles() {
        int start = 0;
        int size = 100;

        List<RoleRepresentation> ret = new ArrayList<>();

        do {
            List<RoleRepresentation> roleRepresentations = getRealm().roles().list(start, size);
            ret.addAll(roleRepresentations);
            start += size;

        } while (CollectionUtils.isNotEmpty(ret) && ret.size() % size == 0);

        return ret;
    }
}