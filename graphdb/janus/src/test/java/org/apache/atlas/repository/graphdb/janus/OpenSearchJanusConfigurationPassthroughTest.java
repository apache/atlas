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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * {@code AtlasJanusGraphDatabase.getConfiguration()} does not set a default for
 * {@code index.search.opensearch.create.ext.mapping.total_fields.limit} itself -- that default (3000) is
 * guaranteed by {@code ApplicationProperties.setDefaults()} before this method ever runs (see
 * {@code ApplicationPropertiesOpenSearchMappingLimitTest}). This test only proves that the Janus subset
 * configuration {@code getConfiguration()} returns is a live pass-through of whatever
 * {@code ApplicationProperties} already contains, i.e. an explicitly-configured value is not lost/altered.
 */
public class OpenSearchJanusConfigurationPassthroughTest {
    @Test
    public void getConfigurationPassesThroughExplicitlyConfiguredMappingLimit() throws Exception {
        PropertiesConfiguration props = new PropertiesConfiguration();
        props.setListDelimiterHandler(new DefaultListDelimiterHandler(','));
        props.addProperty("atlas.graph.index.search.backend", "opensearch");
        props.addProperty("atlas.graph.storage.backend", "berkeleyje");
        props.addProperty("atlas.graph.storage.directory", "/tmp/atlas-janus-passthrough");
        props.addProperty(ApplicationProperties.OPENSEARCH_MAPPING_FIELDS_LIMIT, 4000);

        ApplicationProperties.set(props);
        try {
            Configuration janusConfig = AtlasJanusGraphDatabase.getConfiguration();

            assertEquals(janusConfig.getInt("index.search.opensearch.create.ext.mapping.total_fields.limit"), 4000);
        } finally {
            ApplicationProperties.forceReload();
        }
    }
}
