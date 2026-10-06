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
 */
package org.apache.atlas;

import org.apache.commons.configuration2.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.testng.Assert.assertEquals;

public class ApplicationPropertiesOpenSearchMappingLimitTest {
    @AfterMethod
    public void tearDown() {
        ApplicationProperties.forceReload();
    }

    @Test
    public void setDefaultsInjectsMappingLimitWhenMissingFromFile() throws Exception {
        Path props = Files.createTempFile("atlas-os-default", ".properties");
        Files.writeString(props, ""
                + "atlas.graph.index.search.backend=opensearch\n"
                + "atlas.graph.index.search.hostname=localhost\n"
                + "atlas.graph.index.search.port=9200\n");

        Configuration configuration = ApplicationProperties.get(props.toString());

        assertEquals(configuration.getInt(ApplicationProperties.OPENSEARCH_MAPPING_FIELDS_LIMIT), 3000);

        Configuration janusSubset = ApplicationProperties.getSubsetConfiguration(configuration, "atlas.graph");
        assertEquals(janusSubset.getInt("index.search.opensearch.create.ext.mapping.total_fields.limit"), 3000);
    }

    @Test
    public void fileOverrideIsVisibleInJanusSubset() throws Exception {
        Configuration configuration = ApplicationProperties.get(
                "src/test/resources/atlas-opensearch-mapping-limit-override-test.properties");

        assertEquals(configuration.getInt(ApplicationProperties.OPENSEARCH_MAPPING_FIELDS_LIMIT), 4000);

        Configuration janusSubset = ApplicationProperties.getSubsetConfiguration(configuration, "atlas.graph");
        assertEquals(janusSubset.getInt("index.search.opensearch.create.ext.mapping.total_fields.limit"), 4000);
    }

    @Test
    public void explicitFileValueUsesConfiguredLogPath() throws Exception {
        Configuration configuration = ApplicationProperties.get(
                "src/test/resources/atlas-opensearch-mapping-limit-test.properties");

        assertEquals(configuration.getInt(ApplicationProperties.OPENSEARCH_MAPPING_FIELDS_LIMIT), 3000);

        Configuration janusSubset = ApplicationProperties.getSubsetConfiguration(configuration, "atlas.graph");
        assertEquals(janusSubset.getInt("index.search.opensearch.create.ext.mapping.total_fields.limit"), 3000);
    }
}
