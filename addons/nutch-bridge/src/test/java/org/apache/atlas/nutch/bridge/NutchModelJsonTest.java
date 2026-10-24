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
 * See the License for the specific language governing limitations under
 * the License.
 */
package org.apache.atlas.nutch.bridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertTrue;

public class NutchModelJsonTest {
    @Test
    public void modelDefinesCatalogAndIndexProcessTypes() throws Exception {
        File model = new File("target/models/7000-Nutch/7010-nutch_model.json");
        if (!model.isFile()) {
            model = new File("../models/7000-Nutch/7010-nutch_model.json");
        }

        JsonNode root = new ObjectMapper().readTree(model);
        Set<String> entityNames = new HashSet<>();
        for (JsonNode def : root.get("entityDefs")) {
            entityNames.add(def.get("name").asText());
        }

        assertTrue(entityNames.contains("nutch_crawl"));
        assertTrue(entityNames.contains("nutch_seedlist"));
        assertTrue(entityNames.contains("nutch_segment"));
        assertTrue(entityNames.contains("nutch_domain"));
        assertTrue(entityNames.contains("nutch_host"));
        assertTrue(entityNames.contains("nutch_index_process"));

        Set<String> relNames = new HashSet<>();
        for (JsonNode def : root.get("relationshipDefs")) {
            relNames.add(def.get("name").asText());
        }
        assertTrue(relNames.contains("nutch_crawl_hosts"));
        assertTrue(relNames.contains("nutch_domain_hosts"));

        Set<String> crawlHostAttrs = new HashSet<>();
        for (JsonNode def : root.get("relationshipDefs")) {
            if ("nutch_crawl_hosts".equals(def.get("name").asText())) {
                for (JsonNode attr : def.get("attributeDefs")) {
                    crawlHostAttrs.add(attr.get("name").asText());
                }
            }
        }
        assertTrue(crawlHostAttrs.contains("dnsFailures"));
        assertTrue(crawlHostAttrs.contains("connectionFailures"));
        assertTrue(crawlHostAttrs.contains("goneCount"));
        assertTrue(crawlHostAttrs.contains("redirPermCount"));
        assertTrue(crawlHostAttrs.contains("redirTempCount"));
        assertTrue(crawlHostAttrs.contains("homepageUrl"));
    }
}
