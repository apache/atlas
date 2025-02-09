/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.atlas.connector;

import org.apache.atlas.AtlasClientV2;

import java.util.Map;

public class AtlasConfig {
    private static final Map<String, String> ENV    = System.getenv();
    private static       AtlasClientV2       client;

    public static String[] urls() {
        return new String[] {ENV.getOrDefault("ATLAS_URL", "http://localhost:21000")};
    }

    public static String username() {
        return ENV.getOrDefault("ATLAS_USERNAME", "admin");
    }

    public static String password() {
        return ENV.getOrDefault("ATLAS_PASSWORD", "admin");
    }

    public static String[] auth() {
        return new String[] {username(), password()};
    }

    private AtlasConfig() {
        // to block instantiation
    }

    public static AtlasClientV2 client() {
        if (client == null) {
            client = new AtlasClientV2(urls(), auth());
        }

        return client;
    }

    public static void client(AtlasClientV2 client) {
        AtlasConfig.client = client;
    }
}
