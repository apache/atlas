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
package org.apache.atlas.regression.request;

public class RequestKeys {
    private RequestKeys() {
        throw new AssertionError("Instantiating utility class...");
    }

    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String JSON_CONTENT_TYPE = "application/json";

    public static final String AUTH_COOKIE = "hadoop.auth";
    public static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";
    public static final String COOKIE = "Cookie";
    public static final String WWW_AUTHENTICATE = "WWW-Authenticate";
    public static final String NEGOTIATE = "Negotiate";
    public static final String CURRENT_USER = System
            .getProperty("user.name");
}
