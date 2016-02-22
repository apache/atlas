/*
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
package org.apache.atlas.security;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public interface SecurityProperties {
    String TLS_ENABLED = "atlas.enableTLS";
    String KEYSTORE_FILE_KEY = "keystore.file";
    String DEFAULT_KEYSTORE_FILE_LOCATION = "target/atlas.keystore";
    String KEYSTORE_PASSWORD_KEY = "keystore.password";
    String TRUSTSTORE_FILE_KEY = "truststore.file";
    String DEFATULT_TRUSTORE_FILE_LOCATION = "target/atlas.keystore";
    String TRUSTSTORE_PASSWORD_KEY = "truststore.password";
    String SERVER_CERT_PASSWORD_KEY = "password";
    String CLIENT_AUTH_KEY = "client.auth.enabled";
    String CERT_STORES_CREDENTIAL_PROVIDER_PATH = "cert.stores.credential.provider.path";
    String SSL_CLIENT_PROPERTIES = "ssl-client.xml";
    String BIND_ADDRESS = "atlas.server.bind.address";
    String ATLAS_SSL_EXCLUDE_CIPHER_SUITES = "atlas.ssl.exclude.cipher.suites";
    List<String> DEFAULT_CIPHER_SUITES = Arrays.asList(".*NULL.*", ".*RC4.*", ".*MD5.*",".*DES.*",".*DSS.*");
    
}
