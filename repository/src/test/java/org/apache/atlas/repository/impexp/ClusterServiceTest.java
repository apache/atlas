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

package org.apache.atlas.repository.impexp;

import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasCluster;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.repository.Constants.ATTR_NAME_REFERENCEABLE;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadBaseModel;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ClusterServiceTest {
    private final String TOP_LEVEL_ENTITY_NAME = "db1@cl1";
    private final String CLUSTER_NAME          = "testCl1";
    private final String TARGET_CLUSTER_NAME   = "testCl2";
    private final String QUALIFIED_NAME_STOCKS = "stocks@cl1";
    private final String TYPE_HIVE_DB = "hive_db";

    private AtlasTypeDefStore typeDefStore;
    private AtlasTypeRegistry typeRegistry;
    private ClusterService clusterService;
    private String topLevelEntityGuid = "AAA-BBB-CCC";

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
        AtlasCluster expected = getCluster(CLUSTER_NAME + "_1", TOP_LEVEL_ENTITY_NAME, "EXPORT", 0l, TARGET_CLUSTER_NAME);
        AtlasCluster expected2 = getCluster(TARGET_CLUSTER_NAME + "_1", TOP_LEVEL_ENTITY_NAME, "IMPORT", 0L, TARGET_CLUSTER_NAME);
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
    }

    private AtlasCluster getCluster(String clusterName, String topLevelEntity, String operation, long nextModifiedTimestamp, String targetClusterName) {
        AtlasCluster cluster = new AtlasCluster(clusterName, clusterName);

        Map<String, String> syncMap = new HashMap<>();

        syncMap.put("topLevelEntity", topLevelEntity);
        syncMap.put("operation", operation);
        syncMap.put("nextModifiedTimestamp", Long.toString(nextModifiedTimestamp));
        syncMap.put("targetCluster", targetClusterName);

        cluster.setAdditionalInfo(syncMap);

        return cluster;
    }

    @Test
    public void verifyAdditionalInfo() throws AtlasBaseException {
        final long expectedLastModifiedTimestamp = 200L;

        AtlasCluster expectedCluster = new AtlasCluster(CLUSTER_NAME, CLUSTER_NAME);

        String qualifiedNameAttr = Constants.QUALIFIED_NAME.replace(ATTR_NAME_REFERENCEABLE, "");
        AtlasObjectId objectId = new AtlasObjectId(TYPE_HIVE_DB, qualifiedNameAttr, QUALIFIED_NAME_STOCKS);
        expectedCluster.setAdditionalInfoRepl(topLevelEntityGuid, expectedLastModifiedTimestamp);

        AtlasCluster actualCluster = clusterService.save(expectedCluster);
        assertEquals(actualCluster.getName(), expectedCluster.getName());

        int actualModifiedTimestamp = (int) actualCluster.getAdditionalInfoRepl(topLevelEntityGuid);

        assertEquals(actualModifiedTimestamp, expectedLastModifiedTimestamp);
    }
}
