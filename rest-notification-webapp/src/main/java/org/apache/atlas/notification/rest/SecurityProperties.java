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

package org.apache.atlas.notification.rest;

public final class SecurityProperties {

    private SecurityProperties() {
    }

    public static final String TLS_ENABLED  = "atlas.rest.notification.enableTLS";
    public static final String BIND_ADDRESS = "atlas.rest.notification.server.bind.address";
    public static final String KEYSTORE_PASSWORD_KEY = "keystore.password";
    public static final String TRUSTSTORE_PASSWORD_KEY = "truststore.password";
    public static final String SERVER_CERT_PASSWORD_KEY = "password";

}
