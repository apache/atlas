/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.storm.hook;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.storm.model.StormDataModel;
import org.apache.atlas.storm.model.StormDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.commons.configuration.Configuration;
import org.apache.storm.ILocalCluster;
import org.apache.storm.generated.StormTopology;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class StormAtlasHookIT {

    public static final Logger LOG = LoggerFactory.getLogger(StormAtlasHookIT.class);

    private static final String ATLAS_URL = "http://localhost:21000/";
    private static final String TOPOLOGY_NAME = "word-count";

    private ILocalCluster stormCluster;
    private AtlasClient atlasClient;

    @BeforeClass
    public void setUp() throws Exception {
        // start a local storm cluster
        stormCluster = StormTestUtil.createLocalStormCluster();
        LOG.info("Created a storm local cluster");

        Configuration configuration = ApplicationProperties.get();
        atlasClient = new AtlasClient(configuration.getString("atlas.rest.address", ATLAS_URL));
        registerDataModel(new HiveDataModelGenerator());
    }

    private void registerDataModel(HiveDataModelGenerator dataModelGenerator) throws AtlasException,
            AtlasServiceException {
        try {
            atlasClient.getType(HiveDataTypes.HIVE_PROCESS.getName());
            LOG.info("Hive data model is already registered! Going ahead with registration of Storm Data model");
        } catch(AtlasServiceException ase) {
            if (ase.getStatus() == ClientResponse.Status.NOT_FOUND) {
                //Expected in case types do not exist
                LOG.info("Registering Hive data model");
                atlasClient.createType(dataModelGenerator.getModelAsJson());
            } else {
                throw ase;
            }
        }


        try {
            atlasClient.getType(StormDataTypes.STORM_TOPOLOGY.getName());
        } catch(AtlasServiceException ase) {
            if (ase.getStatus() == ClientResponse.Status.NOT_FOUND) {
                LOG.info("Registering Storm/Kafka data model");
                StormDataModel.main(new String[]{});
                TypesDef typesDef = StormDataModel.typesDef();
                String stormTypesAsJSON = TypesSerialization.toJson(typesDef);
                LOG.info("stormTypesAsJSON = {}", stormTypesAsJSON);
                atlasClient.createType(stormTypesAsJSON);
            }
        }
    }


    @AfterClass
    public void tearDown() throws Exception {
        LOG.info("Shutting down storm local cluster");
        stormCluster.shutdown();

        atlasClient = null;
    }

    @Test
    public void testCreateDataModel() throws Exception {
        StormDataModel.main(new String[]{});
        TypesDef stormTypesDef = StormDataModel.typesDef();

        String stormTypesAsJSON = TypesSerialization.toJson(stormTypesDef);
        LOG.info("stormTypesAsJSON = {}", stormTypesAsJSON);

        registerDataModel(new HiveDataModelGenerator());

        // verify types are registered
        for (StormDataTypes stormDataType : StormDataTypes.values()) {
            Assert.assertNotNull(atlasClient.getType(stormDataType.getName()));
        }
    }

    @Test (dependsOnMethods = "testCreateDataModel")
    public void testAddEntities() throws Exception {
        StormTopology stormTopology = StormTestUtil.createTestTopology();
        StormTestUtil.submitTopology(stormCluster, TOPOLOGY_NAME, stormTopology);
        LOG.info("Submitted topology {}", TOPOLOGY_NAME);

        // todo: test if topology metadata is registered in atlas
        String guid = getTopologyGUID();
        Assert.assertNotNull(guid);
        LOG.info("GUID is {}", guid);

        Referenceable topologyReferenceable = atlasClient.getEntity(guid);
        Assert.assertNotNull(topologyReferenceable);
    }

    private String getTopologyGUID() throws Exception {
        LOG.debug("Searching for topology {}", TOPOLOGY_NAME);
        String query = String.format("from %s where name = \"%s\"",
                StormDataTypes.STORM_TOPOLOGY.getName(), TOPOLOGY_NAME);

        JSONArray results = atlasClient.search(query, 10, 0);
        JSONObject row = results.getJSONObject(0);

        return row.has("$id$") ? row.getJSONObject("$id$").getString("id"): null;
    }
}
