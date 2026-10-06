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
package org.janusgraph.diskstorage.opensearch;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Verifies Atlas {@code atlas.graph.index.search.opensearch.create.ext.*} keys reach
 * {@link OpenSearchSetup#getSettingsFromJanusGraphConf} on the index-scoped JanusGraph configuration.
 */
public class OpenSearchCreateExtrasFromAtlasConfigTest {

    private static final String MAPPING_LIMIT_KEY =
            "index.search.opensearch.create.ext.mapping.total_fields.limit";

    @Test
    public void atlasStyleFlatKeyIsVisibleToOpenSearchCreateExtras() {
        PropertiesConfiguration atlasJanusSubset = new PropertiesConfiguration();
        atlasJanusSubset.setListDelimiterHandler(new DefaultListDelimiterHandler(','));
        atlasJanusSubset.addProperty("index.search.backend", "opensearch");
        atlasJanusSubset.addProperty("index.search.hostname", "localhost");
        atlasJanusSubset.addProperty("index.search.port", 9200);
        atlasJanusSubset.addProperty(MAPPING_LIMIT_KEY, 3000);

        Map<String, Object> settings = readCreateExtras(atlasJanusSubset);

        assertEquals(settings.get("mapping.total_fields.limit").toString(), "3000");
    }

    @Test
    public void atlasGraphSubsetPreservesCreateExtrasForIndexScope() {
        PropertiesConfiguration props = new PropertiesConfiguration();
        props.setListDelimiterHandler(new DefaultListDelimiterHandler(','));
        props.addProperty("atlas.graph.index.search.backend", "opensearch");
        props.addProperty("atlas.graph." + MAPPING_LIMIT_KEY, 3000);

        Configuration janusSubset = props.subset("atlas.graph");

        Map<String, Object> settings = readCreateExtras(janusSubset);

        assertEquals(settings.get("mapping.total_fields.limit").toString(), "3000");
    }

    @Test
    public void propertiesRoundTripPreservesCreateExtrasForIndexScope() {
        PropertiesConfiguration atlasJanusSubset = new PropertiesConfiguration();
        atlasJanusSubset.setListDelimiterHandler(new DefaultListDelimiterHandler(','));
        atlasJanusSubset.addProperty(MAPPING_LIMIT_KEY, 3000);

        Properties flat = ConfigurationConverter.getProperties(atlasJanusSubset);
        Configuration roundTripped = ConfigurationConverter.getConfiguration(flat);

        Map<String, Object> settings = readCreateExtras(roundTripped);

        assertTrue(settings.containsKey("mapping.total_fields.limit"),
                "Expected mapping.total_fields.limit in create.ext settings but got: " + settings);
        assertEquals(settings.get("mapping.total_fields.limit").toString(), "3000");
    }

    private static Map<String, Object> readCreateExtras(Configuration atlasJanusSubset) {
        CommonsConfiguration commons = new CommonsConfiguration(atlasJanusSubset);
        ModifiableConfiguration modifiable = new ModifiableConfiguration(
                GraphDatabaseConfiguration.ROOT_NS,
                commons,
                BasicConfiguration.Restriction.NONE);
        org.janusgraph.diskstorage.configuration.Configuration indexScoped = modifiable.restrictTo("search");
        return OpenSearchSetup.getSettingsFromJanusGraphConf(indexScoped);
    }
}
