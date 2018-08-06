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

package org.apache.atlas.repository.clusterinfo;

import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.clusterinfo.AtlasCluster;
import org.apache.atlas.repository.impexp.ClusterService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadBaseModel;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ClusterServiceTest {
    private final String TOP_LEVEL_ENTITY_NAME = "db1@cl1";
    private final String CLUSTER_NAME          = "testCl1";
    private final String TARGET_CLUSTER_NAME   = "testCl2";

    private AtlasTypeDefStore typeDefStore;
    private AtlasTypeRegistry typeRegistry;
    private ClusterService clusterService;

    @Inject
    public void UserProfileServiceTest(AtlasTypeRegistry typeRegistry,
                                       AtlasTypeDefStore typeDefStore,
                                       ClusterService clusterService) {
        this.typeRegistry = typeRegistry;
        this.typeDefStore = typeDefStore;
        this.clusterService = clusterService;
    }

    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        loadBaseModel(typeDefStore, typeRegistry);
    }

    @Test
    public void saveAndRetrieveClusterInfo() throws AtlasBaseException {
        AtlasCluster expected = getCluster(CLUSTER_NAME, TOP_LEVEL_ENTITY_NAME, "EXPORT", 0l, TARGET_CLUSTER_NAME);
        AtlasCluster expected2 = getCluster(TARGET_CLUSTER_NAME, TOP_LEVEL_ENTITY_NAME, "IMPORT", 0L, TARGET_CLUSTER_NAME);
        AtlasCluster expected3 = getCluster(TARGET_CLUSTER_NAME + "_3", TOP_LEVEL_ENTITY_NAME, "IMPORT", 0, TARGET_CLUSTER_NAME);

        AtlasCluster actual = clusterService.save(expected);
        AtlasCluster actual2 = clusterService.save(expected2);
        AtlasCluster actual3 = clusterService.save(expected3);
        AtlasCluster actual2x = clusterService.get(expected2);

        assertNotNull(actual.getGuid());
        assertNotNull(actual2.getGuid());
        assertNotEquals(actual.getGuid(), actual2.getGuid());
        assertNotEquals(actual2.getGuid(), actual3.getGuid());

        assertEquals(actual2.getGuid(), actual2x.getGuid());


        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getQualifiedName(), expected.getQualifiedName());
        assertEquals(getAdditionalInfo(actual, TOP_LEVEL_ENTITY_NAME).get(AtlasCluster.OPERATION),
                    getAdditionalInfo(expected, TOP_LEVEL_ENTITY_NAME).get(AtlasCluster.OPERATION));

        assertEquals(getAdditionalInfo(actual, TOP_LEVEL_ENTITY_NAME).get(AtlasCluster.NEXT_MODIFIED_TIMESTAMP),
                    getAdditionalInfo(expected, TOP_LEVEL_ENTITY_NAME).get(AtlasCluster.NEXT_MODIFIED_TIMESTAMP));
    }

    private AtlasCluster getCluster(String name, String topLevelEntity, String operation, long nextModifiedTimestamp, String targetClusterName) {
        AtlasCluster cluster = new AtlasCluster(name, name);

        Map<String, Object> syncMap = new HashMap<>();
        syncMap.put("operation", operation);
        syncMap.put("nextModifiedTimestamp", nextModifiedTimestamp);
        syncMap.put("targetCluster", targetClusterName);

        String syncMapJson = AtlasType.toJson(syncMap);
        String topLevelEntitySpecificKey = getTopLevelEntitySpecificKey(topLevelEntity);
        cluster.setAdditionalInfo(topLevelEntitySpecificKey, syncMapJson);
        return cluster;
    }

    private Map<String, Object> getAdditionalInfo(AtlasCluster cluster, String topLevelEntityName) {
        String topLevelEntitySpecificKey = getTopLevelEntitySpecificKey(topLevelEntityName);
        assertTrue(cluster.getAdditionalInfo().containsKey(topLevelEntitySpecificKey));

        String json = cluster.getAdditionalInfo(topLevelEntitySpecificKey);
        return AtlasType.fromJson(json, Map.class);
    }

    private String getTopLevelEntitySpecificKey(String topLevelEntity) {
        return String.format("%s:%s", AtlasCluster.SYNC_INFO_KEY, topLevelEntity);
    }
}
